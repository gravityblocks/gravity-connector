use std::mem::MaybeUninit;

use flux::type_hash::TypeHash;
use serde::{Deserialize, Serialize};
use wincode::{
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
    config::ConfigCore,
    error::invalid_tag_encoding,
    io::{Reader, Writer},
};

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[repr(C)]
pub enum FfiOption<T> {
    Some(T),
    #[default]
    None,
}
impl<T> FfiOption<T> {
    pub const fn is_some(&self) -> bool {
        matches!(self, Self::Some(_))
    }
    pub const fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}
impl<T> From<FfiOption<T>> for Option<T> {
    fn from(val: FfiOption<T>) -> Self {
        match val {
            FfiOption::Some(inner) => Some(inner),
            FfiOption::None => None,
        }
    }
}
impl<T> From<Option<T>> for FfiOption<T> {
    fn from(value: Option<T>) -> Self {
        value.map_or(Self::None, Self::Some)
    }
}
// Can't just derive the following traits, because the derives only work if T is
// always bound to impl that trait
impl<T: TypeHash> TypeHash for FfiOption<T> {
    const TYPE_HASH: u64 = !T::TYPE_HASH; // lol
}
unsafe impl<'de, T, C> SchemaRead<'de, C> for FfiOption<T>
where
    T: SchemaRead<'de, C>,
    C: ConfigCore,
{
    type Dst = FfiOption<T::Dst>;

    fn read(
        mut reader: impl Reader<'de>,
        dst: &mut MaybeUninit<FfiOption<T::Dst>>,
    ) -> ReadResult<()> {
        let variant = <u8 as SchemaRead<'de, C>>::get(reader.by_ref())?;
        match variant {
            0 => dst.write(FfiOption::None),
            1 => dst.write(FfiOption::Some(T::get(reader)?)),
            _ => return Err(invalid_tag_encoding(variant as usize)),
        };
        Ok(())
    }
}

unsafe impl<T, C> SchemaWrite<C> for FfiOption<T>
where
    T: SchemaWrite<C>,
    T::Src: Sized,
    C: ConfigCore,
{
    type Src = FfiOption<T::Src>;

    fn size_of(src: &FfiOption<T::Src>) -> WriteResult<usize> {
        match src {
            FfiOption::Some(value) => Ok(1 + T::size_of(value)?),
            FfiOption::None => Ok(1),
        }
    }

    fn write(mut writer: impl Writer, src: &FfiOption<T::Src>) -> WriteResult<()> {
        match src {
            FfiOption::Some(value) => {
                <u8 as SchemaWrite<C>>::write(writer.by_ref(), &1)?;
                T::write(writer, value)
            }
            FfiOption::None => <u8 as SchemaWrite<C>>::write(writer, &0),
        }
    }
}
