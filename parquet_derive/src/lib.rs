// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#![recursion_limit = "128"]

extern crate proc_macro;
extern crate proc_macro2;
extern crate syn;
#[macro_use]
extern crate quote;

extern crate parquet;

use syn::{
  parse_macro_input, AngleBracketedGenericArguments, Data, DataStruct, DeriveInput,
  Field, Fields, Ident, Lifetime, Path, PathArguments, Type,
};

/// Derive flat, simple RecordWriter implementations. Works by parsing
/// a struct tagged with `#[derive(ParquetRecordWriter)]` and emitting
/// the correct writing code for each field of the struct, in
/// the order fields are defined.
///
/// It is up to the programmer to keep the order of the struct
/// fields lined up with the schema.
///
/// Example:
///
/// ```
/// use parquet;
/// use parquet::record::RecordWriter;
//
/// #[derive(ParquetRecordWriter)]
/// struct ACompleteRecord<'a> {
///   pub a_bool: bool,
///   pub a_str: &'a str,
/// }
/// ```
///

#[derive(Debug)]
struct FieldInfo {
  field_name: Ident,
  field_lifetime: Option<Lifetime>,
  field_type: Ident,
  is_option: bool,
  column_writer_variant: proc_macro2::TokenStream,
}

#[proc_macro_derive(ParquetRecordWriter)]
pub fn parquet_record_writer(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
  let input: DeriveInput = parse_macro_input!(input as DeriveInput);
  let fields: Fields = match input.data {
    Data::Struct(DataStruct { fields, .. }) => fields,
    Data::Enum(_) => unimplemented!("don't support enum"),
    Data::Union(_) => unimplemented!("don't support union"),
  };

  let field_infos: Vec<FieldInfo> =
    fields.iter().map(move |f| FieldInfo::from(f)).collect();

  let writer_snippets: Vec<proc_macro2::TokenStream> =
    field_infos.iter().map(|x| x.to_writer_snippet()).collect();

  let derived_for = input.ident;
  let generics = input.generics;

  (quote! {
      impl#generics RecordWriter<#derived_for#generics> for &[#derived_for#generics] {
        fn write_to_row_group(&self, row_group_writer: &mut Box<parquet::file::writer::RowGroupWriter>) {
           let mut row_group_writer = row_group_writer;
        #(
          {
              let mut column_writer = row_group_writer.next_column().unwrap().unwrap();
              #writer_snippets
              row_group_writer.close_column(column_writer).unwrap();
          }
        );*
        }
      }
  }).into()
}

impl FieldInfo {
  fn from(f: &Field) -> Self {
    let (field_type, field_lifetime, is_option) = match &f.ty {
      Type::Slice(_) => unimplemented!("unsupported type: Slice"),
      Type::Array(_) => unimplemented!("unsupported type: Array"),
      Type::Ptr(_) => unimplemented!("unsupported type: Ptr"),
      Type::Reference(ref tr) => FieldInfo::extract_type_reference_info(tr),
      Type::BareFn(_) => unimplemented!("unsupported type: BareFn"),
      Type::Never(_) => unimplemented!("unsupported type: Never"),
      Type::Tuple(_) => unimplemented!("unsupported type: Tuple"),
      Type::Path(tp) => {
        let (ident, is_option, _) = FieldInfo::extract_path_info(&tp);
        (ident, None, is_option)
      },
      Type::TraitObject(_) => unimplemented!("unsupported type: TraitObject"),
      Type::ImplTrait(_) => unimplemented!("unsupported type: ImplTrait"),
      Type::Paren(_) => unimplemented!("unsupported type: Paren"),
      Type::Group(_) => unimplemented!("unsupported type: Group"),
      Type::Infer(_) => unimplemented!("unsupported type: Infer"),
      Type::Macro(_) => unimplemented!("unsupported type: Macro"),
      Type::Verbatim(_) => unimplemented!("unsupported type: Verbatim"),
    };

    let column_writer_variant = match &field_type.to_string()[..] {
      "bool" => quote! { parquet::column::writer::ColumnWriter::BoolColumnWriter },
      "str" | "String" => {
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
      o => unimplemented!("don't know {} for {:#?}", o, f),
    };

    FieldInfo {
      field_name: f.ident.clone().expect("must be a named field"),
      field_lifetime,
      field_type,
      is_option,
      column_writer_variant,
    }
  }

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
  /// struct UnsupportedRecord {
  ///   a_record: Record
  /// }
  ///
  /// because this parsing logic is not sophisticated enough for definition
  /// levels beyond 1.
  fn to_writer_snippet(&self) -> proc_macro2::TokenStream {
    let field_name = self.field_name.clone();
    let field_type = self.field_type.clone();
    let column_writer_variant = self.column_writer_variant.clone();
    let is_option = self.is_option;

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
  ) -> (Ident, bool, Option<PathArguments>)
  {
    let seg = segments
      .iter()
      .next()
      .expect("must have at least 1 segment");

    let second_level = match &seg.arguments {
      PathArguments::None => None,
      PathArguments::AngleBracketed(AngleBracketedGenericArguments {
        ref args, ..
      }) => {
        let generic_argument = args.iter().next().unwrap();
        match generic_argument {
          syn::GenericArgument::Lifetime(_) => {
            unimplemented!("generic argument lifetime")
          },
          syn::GenericArgument::Type(Type::Reference(ref tr)) => {
            let (ident2, lifetime2, is_option2) =
              FieldInfo::extract_type_reference_info(tr);
            Some((ident2, lifetime2, is_option2))
          },
          syn::GenericArgument::Type(Type::Path(ref tp)) => {
            let (ident2, _is_option2, _path_arguments) = FieldInfo::extract_path_info(tp);
            Some((ident2, None, false))
          },
          syn::GenericArgument::Type(_) => {
            unimplemented!("generic argument type not reference/path")
          },
          syn::GenericArgument::Binding(_) => unimplemented!("generic argument binding"),
          syn::GenericArgument::Constraint(_) => {
            unimplemented!("generic argument constraint")
          },
          syn::GenericArgument::Const(_) => unimplemented!("generic argument const"),
        }
      },
      PathArguments::Parenthesized(_) => unimplemented!("parenthesized"),
    };

    if &seg.ident.to_string()[..] == "Option" {
      if let Some((ident2, _lifetime2, _is_option2)) = second_level {
        (ident2, true, None)
      } else {
        unimplemented!("I couldn't parse what was inside of option")
      }
    } else {
      (seg.ident.clone(), false, Some(seg.arguments.clone()))
    }
  }

  fn extract_type_reference_info(
    &syn::TypeReference {
      ref lifetime,
      ref elem,
      ..
    }: &syn::TypeReference,
  ) -> (Ident, Option<Lifetime>, bool)
  {
    if let Type::Path(ref type_path) = elem.as_ref() {
      let (ident, is_option, _) = FieldInfo::extract_path_info(type_path);
      (ident, lifetime.clone(), is_option)
    } else {
      unimplemented!("unsupported elem {:#?}", elem)
    }
  }
}
