#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Mode {
    ReadOnly,
    ReadWrite,
    IndexRebuilding,
}
