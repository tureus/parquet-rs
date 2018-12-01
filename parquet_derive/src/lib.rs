#![feature(impl_trait_in_bindings)]
#![recursion_limit="128"]

extern crate proc_macro;
extern crate proc_macro2;
extern crate syn;
#[macro_use]
extern crate quote;

extern crate parquet;

#[allow(unused_imports)]
use syn::{ parse_macro_input, DeriveInput, Fields, Ident, Data, DataStruct, Type };

#[proc_macro_derive(ParquetRecordWriter)]
pub fn parquet_record_writer(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input : DeriveInput = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;
    let fields : Fields = match input.data {
        Data::Struct(DataStruct{fields, ..}) => {
            fields
        },
        Data::Enum(_) => unimplemented!(),
        Data::Union(_) => unimplemented!(),
    };

    let attrs = fields.iter().map(|f| {
        f.ident.as_ref().unwrap()
    });

    let attrs_types : Vec<proc_macro2::Ident> = fields.iter().map(move |f| {
        match &f.ty {
            Type::Slice(_) => unimplemented!("boom: Slice"),
            Type::Array(_) => unimplemented!("boom: Array"),
            Type::Ptr(_) => unimplemented!("boom: Ptr"),
            Type::Reference(a) => {
                unimplemented!("boom: Reference {:#?}", a)
            },
            Type::BareFn(_) => unimplemented!("boom: BareFn"),
            Type::Never(_) => unimplemented!("boom: Never"),
            Type::Tuple(_) => unimplemented!("boom: Tuple"),
            Type::Path(syn::TypePath{ path: syn::Path{ segments /* syn::punctuated::Punctuated{inner,..} */, .. }, ..} ) => {
                let entry = segments.iter().next().unwrap();
                entry.ident.clone()
//                let sample_bool = quote! {
//                    bool
//                };
//                unimplemented!("{:#?} `{}`, `{}`", entry.ident == sample_bool.to_string(), entry.ident, sample_bool)
            },
            Type::TraitObject(_) => unimplemented!("boom: TraitObject"),
            Type::ImplTrait(_) => unimplemented!("boom: ImplTrait"),
            Type::Paren(_) => unimplemented!("boom: Paren"),
            Type::Group(_) => unimplemented!("boom: Group"),
            Type::Infer(_) => unimplemented!("boom: Infer"),
            Type::Macro(_) => unimplemented!("boom: Macro"),
            Type::Verbatim(_) => unimplemented!("boom: Verbatim"),
        }
    }).collect();

    let _typed_writer_types : Vec<proc_macro2::TokenStream> = fields.iter().map(move |f| {
        match &f.ty {
            Type::Slice(_) => unimplemented!("boom: Slice"),
            Type::Array(_) => unimplemented!("boom: Array"),
            Type::Ptr(_) => unimplemented!("boom: Ptr"),
            Type::Reference(_) => unimplemented!("boom: Reference"),
            Type::BareFn(_) => unimplemented!("boom: BareFn"),
            Type::Never(_) => unimplemented!("boom: Never"),
            Type::Tuple(_) => unimplemented!("boom: Tuple"),
            Type::Path(syn::TypePath{ path: syn::Path{ segments /* syn::punctuated::Punctuated{inner,..} */, .. }, ..} ) => {
                let entry = segments.iter().next().unwrap();
                let string = entry.ident.to_string();
                match &string[..] {
                    "bool" => quote!{ parquet::data_type::BoolType },
                    o => unimplemented!("don't know {}", o)
                }
            },
            Type::TraitObject(_) => unimplemented!("boom: TraitObject"),
            Type::ImplTrait(_) => unimplemented!("boom: ImplTrait"),
            Type::Paren(_) => unimplemented!("boom: Paren"),
            Type::Group(_) => unimplemented!("boom: Group"),
            Type::Infer(_) => unimplemented!("boom: Infer"),
            Type::Macro(_) => unimplemented!("boom: Macro"),
            Type::Verbatim(_) => unimplemented!("boom: Verbatim"),
        }
    }).collect();

    let column_writer_variant : Vec<proc_macro2::TokenStream> = fields.iter().map(move |f| {
        match &f.ty {
            Type::Slice(_) => unimplemented!("boom: Slice"),
            Type::Array(_) => unimplemented!("boom: Array"),
            Type::Ptr(_) => unimplemented!("boom: Ptr"),
            Type::Reference(_) => unimplemented!("boom: Reference"),
            Type::BareFn(_) => unimplemented!("boom: BareFn"),
            Type::Never(_) => unimplemented!("boom: Never"),
            Type::Tuple(_) => unimplemented!("boom: Tuple"),
            Type::Path(syn::TypePath{ path: syn::Path{ segments /* syn::punctuated::Punctuated{inner,..} */, .. }, ..} ) => {
                let entry = segments.iter().next().unwrap();
                let string = entry.ident.to_string();
                match &string[..] {
                    "bool" => quote!{ parquet::column::writer::ColumnWriter::BoolColumnWriter },
                    o => unimplemented!("don't know {}", o)
                }
            },
            Type::TraitObject(_) => unimplemented!("boom: TraitObject"),
            Type::ImplTrait(_) => unimplemented!("boom: ImplTrait"),
            Type::Paren(_) => unimplemented!("boom: Paren"),
            Type::Group(_) => unimplemented!("boom: Group"),
            Type::Infer(_) => unimplemented!("boom: Infer"),
            Type::Macro(_) => unimplemented!("boom: Macro"),
            Type::Verbatim(_) => unimplemented!("boom: Verbatim"),
        }
    }).collect();

    let expanded = quote! {
        impl RecordWriter<#ident> for &[#ident] {
            fn write_to_row_group(&self, row_group_writer: &mut Box<parquet::file::writer::RowGroupWriter>) {
               let mut row_group_writer = row_group_writer;
            #(
                {
                    let vals : Vec<#attrs_types> = self.iter().map(|x| x.#attrs).collect();
                    let mut column_writer = row_group_writer.next_column().unwrap().unwrap();
                    if let #column_writer_variant(ref mut typed) = column_writer {
                        typed.write_batch(&vals[..], None, None).unwrap();

                    }
                    row_group_writer.close_column(column_writer).unwrap();
                    // TODO: the below did not work, I could not get the row_group to close the typed_writer
                    // perhaps this method is only intended for testing? should it be behind a #[cfg(test)]?
                    // let mut typed_writer : parquet::column::writer::ColumnWriterImpl<#typed_writer_types> = parquet::column::writer::get_typed_column_writer(column_writer);
                    // typed_writer.write_batch(&vals[..], None, None).unwrap();
                    // typed_writer.close().unwrap();
                }
            );*
            }
        }
    };

    // Hand the output tokens back to the compiler
    proc_macro::TokenStream::from(expanded)
}