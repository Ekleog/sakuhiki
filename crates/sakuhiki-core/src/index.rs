use crate::{Backend, Datum, IndexError};

pub trait Index<B: Backend> {
    type Datum: Datum<B>;

    fn cf(&self) -> &'static str;

    fn index<'t>(
        &self,
        datum: &Self::Datum,
        transaction: &mut B::RwTransaction<'t>,
        cf: &mut B::RwTransactionCf<'t>,
    ) -> Result<(), B::Error>;

    fn unindex<'t>(
        &self,
        datum: &Self::Datum,
        transaction: &mut B::RwTransaction<'t>,
        cf: &mut B::RwTransactionCf<'t>,
    ) -> Result<(), B::Error>;

    fn index_from_slice<'t>(
        &self,
        slice: &[u8],
        transaction: &mut B::RwTransaction<'t>,
        cf: &mut B::RwTransactionCf<'t>,
    ) -> Result<(), IndexError<B, Self::Datum>> {
        let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
        self.index(&datum, transaction, cf)
            .map_err(IndexError::Backend)
    }

    fn unindex_from_slice<'t>(
        &self,
        slice: &[u8],
        transaction: &mut B::RwTransaction<'t>,
        cf: &mut B::RwTransactionCf<'t>,
    ) -> Result<(), IndexError<B, Self::Datum>> {
        let datum = Self::Datum::from_slice(slice).map_err(IndexError::Parsing)?;
        self.unindex(&datum, transaction, cf)
            .map_err(IndexError::Backend)
    }
}
