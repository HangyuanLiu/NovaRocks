#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RewritePhase {
    LogicalNormalize,
    StructuralRewrite,
    SemanticRewrite,
    Validation,
}

impl RewritePhase {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LogicalNormalize => "LogicalNormalize",
            Self::StructuralRewrite => "StructuralRewrite",
            Self::SemanticRewrite => "SemanticRewrite",
            Self::Validation => "Validation",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_names_are_stable() {
        assert_eq!(RewritePhase::LogicalNormalize.as_str(), "LogicalNormalize");
        assert_eq!(
            RewritePhase::StructuralRewrite.as_str(),
            "StructuralRewrite"
        );
        assert_eq!(RewritePhase::SemanticRewrite.as_str(), "SemanticRewrite");
        assert_eq!(RewritePhase::Validation.as_str(), "Validation");
    }
}
