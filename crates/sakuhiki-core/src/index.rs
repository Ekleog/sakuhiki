use crate::{Backend, Datum};

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
}
