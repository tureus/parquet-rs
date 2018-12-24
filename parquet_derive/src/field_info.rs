use syn::{
  AngleBracketedGenericArguments,
  Field, Ident, Lifetime, Path, PathArguments, PathSegment, Type,
};

#[derive(Debug)]
pub struct FieldInfo {
  syn_field: Field,
  ident: Ident,
  field_lifetime: Option<Lifetime>,
  field_type: Ident,
  field_generic_arguments: Vec<FieldInfoGenericArg>,
}

impl FieldInfo {
  pub fn from(f: &syn::Field) -> Self {
    let (field_type, field_lifetime, field_generic_arguments) = match &f.ty {
      Type::Slice(_) => unimplemented!("unsupported type: Slice"),
      Type::Array(_) => unimplemented!("unsupported type: Array"),
      Type::Ptr(_) => unimplemented!("unsupported type: Ptr"),
      Type::Reference(ref tr) => {
        let (ft,fl, fga) = FieldInfo::extract_type_reference_info(tr);
        (ft, fl, fga)
      },
      Type::BareFn(_) => unimplemented!("unsupported type: BareFn"),
      Type::Never(_) => unimplemented!("unsupported type: Never"),
      Type::Tuple(_) => unimplemented!("unsupported type: Tuple"),
      Type::Path(tp) => {
        let (ft,fga) = FieldInfo::extract_path_info(&tp);
        (ft, None, fga)
      },
      Type::TraitObject(_) => unimplemented!("unsupported type: TraitObject"),
      Type::ImplTrait(_) => unimplemented!("unsupported type: ImplTrait"),
      Type::Paren(_) => unimplemented!("unsupported type: Paren"),
      Type::Group(_) => unimplemented!("unsupported type: Group"),
      Type::Infer(_) => unimplemented!("unsupported type: Infer"),
      Type::Macro(_) => unimplemented!("unsupported type: Macro"),
      Type::Verbatim(_) => unimplemented!("unsupported type: Verbatim"),
    };

    FieldInfo {
      syn_field: f.clone(),
      ident: f.ident.clone().expect("must be a named field"),
      field_lifetime,
      field_type,
      field_generic_arguments,
    }
  }

  fn column_writer_variant(&self) -> Path {
    let ftype_string = self.field_type.to_string();
    let ftype_string = if &ftype_string[..] == "Option" {
      let fga : &FieldInfoGenericArg = self.field_generic_arguments.get(0).expect("must have at least 1 generic argument");
      fga.field_type.to_string()
    } else {
      ftype_string
    };

    FieldInfo::ident_to_column_writer_variant(&ftype_string[..])
  }

  fn ident_to_column_writer_variant(id: &str) -> Path {
    let column_writer_variant = match id {
      "bool" => quote! { parquet::column::writer::ColumnWriter::BoolColumnWriter },
      "str" | "String" | "Vec" => {
        quote! { parquet::column::writer::ColumnWriter::ByteArrayColumnWriter }
      },
      "i32" | "u32" => {
        quote! { parquet::column::writer::ColumnWriter::Int32ColumnWriter }
      },
      "i64" | "u64" => {
        quote! { parquet::column::writer::ColumnWriter::Int64ColumnWriter }
      },
      "f32" => quote! { parquet::column::writer::ColumnWriter::FloatColumnWriter },
      "f64" => quote! { parquet::column::writer::ColumnWriter::DoubleColumnWriter },
      o => unimplemented!("don't know column writer variant for {}", o),
    };

    syn::parse2(column_writer_variant).unwrap()
  }

  fn is_option(&self) -> bool { self.field_type.to_string() == "Option".to_string() }

  /// Takes the parsed data of the struct and emits a valid
  /// column writer snippet.
  ///
  /// Can only generate writers for basic structs, for example:
  ///
  /// struct Record {
  ///   a_bool: bool,
  ///   maybe_a_bool: Option<bool>
  /// }
  ///
  /// but not
  ///
  /// struct UnsupportedNestedRecord {
  ///   a_property: bool,
  ///   nested_record: Record
  /// }
  ///
  /// because this parsing logic is not sophisticated enough for definition
  /// levels beyond 1.
  pub fn to_writer_snippet(&self) -> proc_macro2::TokenStream {
    let field_name = self.ident.clone();
    let field_type = self.field_type.clone();
    let column_writer_variant = self.column_writer_variant();
    let is_option = self.is_option();

    if is_option {
      let definition_levels = quote! {
        let definition_levels : Vec<i16> = self.iter().
          map(|x| if x.#field_name.is_some() { 1 } else { 0 }).
          collect();
      };

      match &self.field_type.to_string()[..] {
        "str" => quote! {
          {
            #definition_levels
            let vals : Vec<parquet::data_type::ByteArray> = self.iter().
              map(|x| x.#field_name).
              filter_map(|z| {
                if let Some(ref inner) = z {
                    Some((*inner).into())
                } else {
                    None
                }
              }).
              collect();

            if let #column_writer_variant(ref mut typed) = column_writer {
                typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
            }
          }
        },
        // TODO: can this be lumped with str by doing Borrow<str>/AsRef<str> in the
        // ByteArray::from?
        "String" => {
          quote! {
            {
              #definition_levels
              let vals : Vec<parquet::data_type::ByteArray> = self.iter().
                map(|x| &x.#field_name).
                filter_map(|z| {
                  if let Some(ref inner) = z {
                      Some((&inner[..]).into())
                  } else {
                      None
                  }
                }).
                collect();
              if let #column_writer_variant(ref mut typed) = column_writer {
                  typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
              }
            }
          }
        },
        _ => quote! {
          {
            #definition_levels
            let vals : Vec<#field_type> = self.iter().
                    map(|x| x.#field_name).
                    filter(|y| y.is_some()).
                    filter_map(|x| x).
                    collect();

            if let #column_writer_variant(ref mut typed) = column_writer {
                typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
            }
          }
        },
      }
    } else {
      match &self.field_type.to_string()[..] {
        "str" => quote! {
          {
            let vals : Vec<parquet::data_type::ByteArray> = self.iter().map(|x|
                x.#field_name.into()
            ).collect();
            if let #column_writer_variant(ref mut typed) = column_writer {
                typed.write_batch(&vals[..], None, None).unwrap();
            }
          }
        },
        // TODO: can this be lumped with str by doing Borrow<str> in the ByteArray::from?
        "String" => quote! {
          {
            let vals : Vec<parquet::data_type::ByteArray> = self.iter().map(|x|
                (&x.#field_name[..]).into()
            ).collect();
            if let #column_writer_variant(ref mut typed) = column_writer {
                typed.write_batch(&vals[..], None, None).unwrap();
            }
          }
        },
        _ => quote! {
          {
            let vals : Vec<#field_type> = self.iter().map(|x| x.#field_name).collect();
            if let #column_writer_variant(ref mut typed) = column_writer {
                typed.write_batch(&vals[..], None, None).unwrap();
            }
          }
        },
      }
    }
  }

  fn extract_path_info(
    &syn::TypePath {
      path: Path { ref segments, .. },
      ..
    }: &syn::TypePath,
  ) -> (Ident, Vec<FieldInfoGenericArg>)
  {
    let seg : &PathSegment = segments
      .iter()
      .next()
      .expect("must have at least 1 segment");

    let generic_arg = match &seg.arguments {
      PathArguments::None => vec![],
      PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                                      ref args, ..
                                    }) => {
        let generic_argument: &syn::GenericArgument = args.iter().next().unwrap();
        args
          .iter()
          .next()
          .expect("derive only supports one generic argument");
        FieldInfoGenericArg::from_generic_argument(generic_argument)
      },
      PathArguments::Parenthesized(_) => unimplemented!("parenthesized"),
    };

    (seg.ident.clone(), generic_arg)
  }

  fn extract_type_reference_info(
    &syn::TypeReference {
      ref lifetime,
      ref elem,
      ..
    }: &syn::TypeReference,
  ) -> (Ident, Option<Lifetime>, Vec<FieldInfoGenericArg>)
  {
    if let Type::Path(ref type_path) = elem.as_ref() {
      let (ident, generic_args) = FieldInfo::extract_path_info(type_path);
      (ident, lifetime.clone(), generic_args)
    } else {
      unimplemented!("unsupported elem {:#?}", elem)
    }
  }
}

#[derive(Debug)]
struct FieldInfoGenericArg {
  field_type: Ident,
  field_lifetime: Option<Lifetime>,
}

impl FieldInfoGenericArg {
  fn from_generic_argument(arg: &syn::GenericArgument) -> Vec<Self> {
    match arg {
      syn::GenericArgument::Type(Type::Reference(ref tr)) => {
        let (gen_type, gen_lifetime, _fga) = FieldInfo::extract_type_reference_info(tr);
        vec![FieldInfoGenericArg {
          field_type: gen_type,
          field_lifetime: gen_lifetime,
        }]
      },
      syn::GenericArgument::Type(Type::Path(syn::TypePath {
                                              path: Path { ref segments, .. },
                                              ..
                                            })) => {
        let segs: Vec<PathSegment> = segments.clone().into_iter().collect();
        FieldInfoGenericArg::from(segs)
      },
      syn::GenericArgument::Lifetime(_) => unimplemented!("generic arg: lifetime"),
      syn::GenericArgument::Type(_) => {
        unimplemented!("generic arg: only reference/path")
      },
      syn::GenericArgument::Binding(_) => unimplemented!("generic arg: binding"),
      syn::GenericArgument::Constraint(_) => {
        unimplemented!("generic argument: constraint")
      },
      syn::GenericArgument::Const(_) => unimplemented!("generic argument: const"),
    }
  }

  fn from_path_segment(seg: PathSegment) -> Self {
    FieldInfoGenericArg {
      field_type: seg.ident,
      field_lifetime: None,
    }
  }

  pub fn from(segments: Vec<PathSegment>) -> Vec<Self> {
    if segments.len() != 1 {
      unimplemented!("parquet derive only supports fields with 1 generic argument")
    }

    segments
      .into_iter()
      .map(|seg| FieldInfoGenericArg::from_path_segment(seg))
      .collect()
  }
}

#[cfg(test)]
mod test {
  use super::*;
  use syn::{
    self,
    Data, DataStruct, DeriveInput
  };

  fn extract_fields(input: proc_macro2::TokenStream) -> Vec<syn::Field> {
    let input: DeriveInput = syn::parse2(input).unwrap();

    let fields = match input.data {
      Data::Struct(DataStruct { fields, .. }) => fields,
      _ => panic!("input must be a struct"),
    };

    fields.iter().map(|x| x.to_owned()).collect()
  }

  #[test]
  fn field_info_vec() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct VecHolder {
        a_vec: Vec<u8>
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { a_vec }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { Vec }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    assert_eq!(fi.field_generic_arguments.len(), 1);

    let gen: &FieldInfoGenericArg = &fi.field_generic_arguments[0];
    let exp_gen_type: syn::Ident = syn::parse2(quote!{ u8 }).unwrap();
    assert_eq!(gen.field_type, exp_gen_type);
    assert_eq!(gen.field_lifetime, None);
  }

  #[test]
  fn field_info_option() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct OptionHolder {
        the_option: Option<String>
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_option }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { Option }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    assert_eq!(fi.field_generic_arguments.len(), 1);

    let gen: &FieldInfoGenericArg = &fi.field_generic_arguments[0];
    let exp_gen_type: syn::Ident = syn::parse2(quote!{ String }).unwrap();
    assert_eq!(gen.field_type, exp_gen_type);
    assert_eq!(gen.field_lifetime, None);
  }

  #[test]
  fn field_info_borrowed_str() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct StrBorrower<'a> {
        the_str: &'a str
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_str }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { str }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    let exp_field_lifetime: syn::Lifetime = syn::parse2(quote!{ 'a }).unwrap();
    assert_eq!(fi.field_lifetime.as_ref().unwrap(), &exp_field_lifetime);

    assert_eq!(fi.field_generic_arguments.len(), 0);
  }

  #[test]
  fn field_info_borrowed_string() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a> {
        the_string: &'a String
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_string }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { String }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    let exp_field_lifetime: syn::Lifetime = syn::parse2(quote!{ 'a }).unwrap();
    assert_eq!(fi.field_lifetime.as_ref().unwrap(), &exp_field_lifetime);

    assert_eq!(fi.field_generic_arguments.len(), 0);
  }

  #[test]
  fn field_info_option_borrowed_str() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a> {
        the_option_str: Option<&'a str>
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_option_str }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { Option }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    assert_eq!(fi.field_lifetime, None);

    assert_eq!(fi.field_generic_arguments.len(), 1);

    let gen: &FieldInfoGenericArg = &fi.field_generic_arguments[0];

    let exp_gen_type: syn::Ident = syn::parse2(quote! { str }).unwrap();
    assert_eq!(gen.field_type, exp_gen_type);
    let exp_gen_type: syn::Lifetime = syn::parse2(quote! { 'a }).unwrap();
    assert_eq!(gen.field_lifetime.as_ref().unwrap(), &exp_gen_type);
  }

  #[test]
  fn field_info_borrowed_option_string() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a> {
        the_option_str: &'a Option<String>
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_option_str }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { Option }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    let exp_field_lifetime: syn::Lifetime = syn::parse2(quote!{ 'a }).unwrap();
    assert_eq!(fi.field_lifetime.as_ref().unwrap(), &exp_field_lifetime);

    assert_eq!(fi.field_generic_arguments.len(), 1);

    let gen: &FieldInfoGenericArg = &fi.field_generic_arguments[0];

    let exp_gen_type: syn::Ident = syn::parse2(quote! { String }).unwrap();
    assert_eq!(gen.field_type, exp_gen_type);
    assert_eq!(gen.field_lifetime, None);
  }

  #[test]
  fn field_info_borrowed_option_borrowed_str() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a,'b> {
        the_option_str: &'a Option<&'b str>
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_option_str }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { Option }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    let exp_field_lifetime: syn::Lifetime = syn::parse2(quote!{ 'a }).unwrap();
    assert_eq!(fi.field_lifetime.as_ref().unwrap(), &exp_field_lifetime);

    assert_eq!(fi.field_generic_arguments.len(), 1);

    let gen: &FieldInfoGenericArg = &fi.field_generic_arguments[0];

    let exp_gen_type: syn::Ident = syn::parse2(quote! { str }).unwrap();
    assert_eq!(gen.field_type, exp_gen_type);

    let exp_gen_lifetime: syn::Lifetime = syn::parse2(quote! { 'b }).unwrap();
    assert_eq!(gen.field_lifetime.as_ref().unwrap(), &exp_gen_lifetime);
  }

  #[test]
  fn simple_struct_to_writer_snippet() {
    let struct_def: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a,'b> {
        the_string: String
      }
    };

    let fields = extract_fields(struct_def);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let writer_snippet : syn::Expr = syn::parse2(fi.to_writer_snippet()).unwrap();
    let exp_writer_snippet : syn::Expr = syn::parse2(quote!{
      {
        let vals : Vec<parquet::data_type::ByteArray> = self.iter().map(|x| (&x.the_string[..]).into()).collect();

        if let parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(ref mut typed) = column_writer {
            typed.write_batch(&vals[..], None, None).unwrap();
        }
      }
    }).unwrap();

    assert_eq!(writer_snippet, exp_writer_snippet);
  }
}