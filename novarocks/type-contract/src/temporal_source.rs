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
//! Explicit temporal provenance and ordered operational source channels.
//! There is deliberately no Default/unknown fact or function-name dispatch.
use crate::ExpressionUseId;
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TemporalSourceKind {
    TimeToSec,
    TimeFormat,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TemporalCastKind {
    Ordinary,
    Time,
    TimeFromDatetime,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TemporalSourceShape {
    FormatOrdinary,
    FormatUtf8Override,
    SecondsDirect,
    SecondsCastString,
    SecondsCastOther,
    SecondsRoundtrip,
}
impl TemporalSourceShape {
    pub const fn kind(self) -> TemporalSourceKind {
        match self {
            Self::FormatOrdinary | Self::FormatUtf8Override => TemporalSourceKind::TimeFormat,
            _ => TemporalSourceKind::TimeToSec,
        }
    }
    pub const fn source_count(self) -> usize {
        match self {
            Self::FormatOrdinary | Self::SecondsCastString => 2,
            Self::FormatUtf8Override | Self::SecondsCastOther => 3,
            _ => 1,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TemporalSourceFacts {
    FormatOrdinary,
    FormatUtf8Override,
    SecondsDirect { cast_chain: Box<[TemporalCastKind]> },
    SecondsCastString { cast_chain: Box<[TemporalCastKind]> },
    SecondsCastOther { cast_chain: Box<[TemporalCastKind]> },
    SecondsRoundtrip { cast_chain: Box<[TemporalCastKind]> },
}
impl TemporalSourceFacts {
    pub const fn shape(&self) -> TemporalSourceShape {
        match self {
            Self::FormatOrdinary => TemporalSourceShape::FormatOrdinary,
            Self::FormatUtf8Override => TemporalSourceShape::FormatUtf8Override,
            Self::SecondsDirect { .. } => TemporalSourceShape::SecondsDirect,
            Self::SecondsCastString { .. } => TemporalSourceShape::SecondsCastString,
            Self::SecondsCastOther { .. } => TemporalSourceShape::SecondsCastOther,
            Self::SecondsRoundtrip { .. } => TemporalSourceShape::SecondsRoundtrip,
        }
    }
    pub fn cast_chain(&self) -> &[TemporalCastKind] {
        match self {
            Self::FormatOrdinary | Self::FormatUtf8Override => &[],
            Self::SecondsDirect { cast_chain }
            | Self::SecondsCastString { cast_chain }
            | Self::SecondsCastOther { cast_chain }
            | Self::SecondsRoundtrip { cast_chain } => cast_chain,
        }
    }
    pub fn validate(&self) -> Result<(), TemporalSourceError> {
        if self.cast_chain().len() > crate::MAX_CONTROL_DEPTH {
            return Err(TemporalSourceError::TooDeep);
        }
        if matches!(
            self,
            Self::SecondsCastString { .. } | Self::SecondsCastOther { .. }
        ) && self.cast_chain().is_empty()
        {
            return Err(TemporalSourceError::MissingCast);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TemporalSourceRole {
    Normal,
    Format,
    RawOverride,
    OriginalSeconds,
    ImmediateCastSource,
    DeepestCastSource,
}
impl TemporalSourceShape {
    pub const fn roles(self) -> [Option<TemporalSourceRole>; 3] {
        use TemporalSourceRole::*;
        match self {
            Self::FormatOrdinary => [Some(Normal), Some(Format), None],
            Self::FormatUtf8Override => [Some(RawOverride), Some(Normal), Some(Format)],
            Self::SecondsDirect => [Some(Normal), None, None],
            Self::SecondsCastString => [Some(Normal), Some(ImmediateCastSource), None],
            Self::SecondsCastOther => [
                Some(Normal),
                Some(ImmediateCastSource),
                Some(DeepestCastSource),
            ],
            Self::SecondsRoundtrip => [Some(OriginalSeconds), None, None],
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalSourceDefinitions<D> {
    pub facts: TemporalSourceFacts,
    pub definitions: Box<[D]>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalSourceOccurrence<D> {
    pub role: TemporalSourceRole,
    pub use_id: ExpressionUseId,
    pub definition: D,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalSourcePlan<D> {
    pub facts: TemporalSourceFacts,
    pub sources: Box<[TemporalSourceOccurrence<D>]>,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TemporalSourceError {
    TooDeep,
    MissingCast,
    WrongCount,
    WrongRole,
    SharedOccurrence,
    WrongDefinition,
}
impl<D> TemporalSourcePlan<D> {
    /// Authenticate the nominal channel grammar before any source is demanded.
    /// Definition correspondence belongs to the original author/decoder.
    pub fn validate_structure(&self) -> Result<(), TemporalSourceError> {
        self.facts.validate()?;
        if self.sources.len() != self.facts.shape().source_count() {
            return Err(TemporalSourceError::WrongCount);
        }
        let roles = self.facts.shape().roles();
        for (i, source) in self.sources.iter().enumerate() {
            if Some(source.role) != roles[i] {
                return Err(TemporalSourceError::WrongRole);
            }
            if self.sources[..i]
                .iter()
                .any(|other| other.use_id == source.use_id)
            {
                return Err(TemporalSourceError::SharedOccurrence);
            }
        }
        Ok(())
    }
}
impl<D: PartialEq> TemporalSourcePlan<D> {
    pub fn validate(
        &self,
        definitions: &TemporalSourceDefinitions<D>,
    ) -> Result<(), TemporalSourceError> {
        self.validate_structure()?;
        if self.facts != definitions.facts
            || self.sources.len() != self.facts.shape().source_count()
            || self.sources.len() != definitions.definitions.len()
        {
            return Err(TemporalSourceError::WrongCount);
        }
        let roles = self.facts.shape().roles();
        for (i, source) in self.sources.iter().enumerate() {
            if Some(source.role) != roles[i] {
                return Err(TemporalSourceError::WrongRole);
            }
            if source.definition != definitions.definitions[i] {
                return Err(TemporalSourceError::WrongDefinition);
            }
            if self.sources[..i]
                .iter()
                .any(|other| other.use_id == source.use_id)
            {
                return Err(TemporalSourceError::SharedOccurrence);
            }
        }
        Ok(())
    }
}
