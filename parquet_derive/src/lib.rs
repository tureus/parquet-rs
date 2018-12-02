#![feature(impl_trait_in_bindings, box_patterns)]
#![recursion_limit="128"]

extern crate proc_macro;
extern crate proc_macro2;
extern crate syn;
#[macro_use]
extern crate quote;

extern crate parquet;

#[allow(unused_imports)]
use syn::{ parse_macro_input, DeriveInput, Fields, Field, Ident, Data, DataStruct, Type, TypePath, Path, Generics, Lifetime, PathArguments, AngleBracketedGenericArguments, ParenthesizedGenericArguments};

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
    let input : DeriveInput = parse_macro_input!(input as DeriveInput);
    let fields : Fields = match input.data {
        Data::Struct(DataStruct{fields, ..}) => {
            fields
        },
        Data::Enum(_) => unimplemented!("don't support enum"),
        Data::Union(_) => unimplemented!("don't support union"),
    };

    let field_infos : Vec<FieldInfo> = fields.iter().map(move |f| {
        FieldInfo::from(f)
    }).collect();

    // Hand the output tokens back to the compiler
    proc_macro::TokenStream::from(writer_from_field_infos(input.ident, input.generics,&field_infos[..]))
}

fn extract_type_reference_info(&syn::TypeReference{ref lifetime, ref elem, ..}: &syn::TypeReference) -> (Ident,Option<Lifetime>,bool) {
    if let Type::Path(ref type_path) = elem.as_ref() {
        let (ident,is_option, _) = extract_path_info(type_path);
        (ident,lifetime.clone(),is_option)
    } else {
        unimplemented!("unsupported elem {:#?}", elem)
    }
}

fn extract_path_info(&syn::TypePath{path: Path{ref segments,..}, ..}: &syn::TypePath) -> (Ident,bool,Option<PathArguments>) {
    let seg = segments.iter().next().expect("must have at least 1 segment");

    let second_level = match &seg.arguments {
        PathArguments::None => None,
        PathArguments::AngleBracketed(AngleBracketedGenericArguments{ref args, ..}) => {
            let generic_argument = args.iter().next().unwrap();
            match generic_argument {
                syn::GenericArgument::Lifetime(_) => unimplemented!("generic argument lifetime"),
                syn::GenericArgument::Type(Type::Reference(ref tr)) => {
                    let (ident2,lifetime2,is_option2) = extract_type_reference_info(tr);
                    Some((ident2,lifetime2,is_option2))
                },
                syn::GenericArgument::Type(Type::Path(_)) => unimplemented!("generic argument type path"),
                syn::GenericArgument::Type(_) => unimplemented!("generic argument type not reference/path"),
                syn::GenericArgument::Binding(_) => unimplemented!("generic argument binding"),
                syn::GenericArgument::Constraint(_) => unimplemented!("generic argument constraint"),
                syn::GenericArgument::Const(_) => unimplemented!("generic argument const"),
            }
        },
        PathArguments::Parenthesized(_) => unimplemented!("parenthesized")
    };

    if &seg.ident.to_string()[..] == "Option" {
        if let Some((ident2,lifetime2,is_option2)) = second_level {
            (ident2,true,None)
        } else {
            unimplemented!("I couldn't parse what was inside of option")
        }
    } else {
        (seg.ident.clone(),false,Some(seg.arguments.clone()))
    }
}

impl FieldInfo {
    fn from(f: &Field) -> Self {
        let (field_type,field_lifetime, is_option) = match &f.ty {
            Type::Slice(_) => unimplemented!("boom: Slice"),
            Type::Array(_) => unimplemented!("boom: Array"),
            Type::Ptr(_) => unimplemented!("boom: Ptr"),
            Type::Reference(ref tr) => {
//                unimplemented!("lifetime: {:#?}", lifetime);
                extract_type_reference_info(tr)
            },
            Type::BareFn(_) => unimplemented!("boom: BareFn"),
            Type::Never(_) => unimplemented!("boom: Never"),
            Type::Tuple(_) => unimplemented!("boom: Tuple"),
            Type::Path(tp) => {
                let (ident,is_option, _) = extract_path_info(&tp);
                (ident,None,is_option)
            },
            Type::TraitObject(_) => unimplemented!("boom: TraitObject"),
            Type::ImplTrait(_) => unimplemented!("boom: ImplTrait"),
            Type::Paren(_) => unimplemented!("boom: Paren"),
            Type::Group(_) => unimplemented!("boom: Group"),
            Type::Infer(_) => unimplemented!("boom: Infer"),
            Type::Macro(_) => unimplemented!("boom: Macro"),
            Type::Verbatim(_) => unimplemented!("boom: Verbatim"),
        };

        let column_writer_variant = match &field_type.to_string()[..] {
            "bool" => quote!{ parquet::column::writer::ColumnWriter::BoolColumnWriter },
            "str" | "String"  => quote!{ parquet::column::writer::ColumnWriter::ByteArrayColumnWriter },
            o      => unimplemented!("don't know {} for {:#?}", o, f)
        };

        let field_type_with_lifetime = if field_lifetime.is_none() {
            quote! { #field_type }
        } else {
            quote! { &#field_lifetime #field_type }
        };

        FieldInfo {
            field_name: f.ident.clone().expect("must be a named field"),
            field_lifetime,
            field_type,
            is_option,
            column_writer_variant
        }
    }

    pub fn to_writer_snippet(&self) -> proc_macro2::TokenStream {
        let field_name = self.field_name.clone();
        let field_type = self.field_type.clone();
        let column_writer_variant = self.column_writer_variant.clone();
        let is_option = self.is_option;

        if is_option {
            match &self.field_type.to_string()[..] {
                "str" => quote! {
                    {
                        let definition_levels : Vec<i16> = self.iter().
                                map(|x| x.#field_name).
                                map(|y| if y.is_some() { 1 } else { 0 }).
                                collect();
                        let vals : Vec<parquet::data_type::ByteArray> = self.iter().
                                map(|x| x.#field_name).
                                filter(|y| y.is_some()).
                                filter_map(|z| z).
                                map(|x|
                                    x.into()
                                ).collect();
                        if let #column_writer_variant(ref mut typed) = column_writer {
                            typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
                        }
                    }
                },
                    // TODO: can this be lumped with str by doing Borrow<str>/AsRef<str> in the ByteArray::from?
                "String" => quote! {
                    {
                        let definition_levels : Vec<i16> = self.iter().
                                map(|x| x.#field_name).
                                map(|y| if y.is_some() { 1 } else { 0 }).
                                collect();
                        let vals : Vec<parquet::data_type::ByteArray> = self.iter().
                                map(|x| x.#field_name).
                                filter(|y| y.is_some()).
                                filter_map(|z| z).
                                map(|x|
                                    (&x[..]).into()
                                ).collect();
                        if let #column_writer_variant(ref mut typed) = column_writer {
                            typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
                        }
                    }
                },
                _ => quote! {
                    {
                        let filtered_vals : Vec<_> = self.iter().map(|x| x.#field_name).filter(|y| y.is_some()).filter_map().collect();
                        let definition_levels : Vec<i16> = self.iter().map(|x| x.#field_name).map(|y| if y.is_some() { 1 } else { 0 }).collect();

                        if let #column_writer_variant(ref mut typed) = column_writer {
                            typed.write_batch(&filtered_vals[..], Some(&definition_levels[..]), None).unwrap();
                        }
                    }
                }
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
                }
            }

        }
    }
}



fn writer_from_field_infos(derived_for: Ident, generics: Generics, field_infos: &[FieldInfo]) -> proc_macro2::TokenStream {
    let field_names : Vec<Ident> = field_infos.iter().map(|x| x.field_name.clone()).collect();
    let field_types : Vec<Ident> = field_infos.iter().map(|x| x.field_type.clone()).collect();
    let column_writer_variant : Vec<proc_macro2::TokenStream> = field_infos.iter().map(|x| x.column_writer_variant.clone()).collect();
    let writer_snippets : Vec<proc_macro2::TokenStream> = field_infos.iter().map(|x| x.to_writer_snippet()).collect();

    quote! {
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
    }
}