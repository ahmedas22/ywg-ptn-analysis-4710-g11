"""Tests for association rule mining module."""

import pandas as pd
import pytest


def test_apriori_on_synthetic_data():
    """Apriori discovers expected rule from synthetic boolean data."""
    mlxtend = pytest.importorskip("mlxtend")
    from mlxtend.frequent_patterns import apriori, association_rules

    # Synthetic: high_poverty always co-occurs with low_access
    data = pd.DataFrame({
        "high_poverty": [True, True, True, True, False, False, False, False],
        "low_access": [True, True, True, True, False, False, True, False],
        "high_density": [True, False, True, False, True, False, True, False],
    })
    data = data.astype(bool)

    frequent = apriori(data, min_support=0.25, use_colnames=True)
    assert not frequent.empty

    rules = association_rules(frequent, metric="confidence", min_threshold=0.5)
    assert not rules.empty

    # Verify at least one rule exists with lift >= 1
    assert (rules["lift"] >= 1.0).any()


def test_boolean_casting():
    """Boolean casting preserves truth values for Apriori input."""
    df = pd.DataFrame({
        "a": [1, 0, 1, 0],
        "b": [True, False, True, False],
        "c": [1.0, 0.0, 1.0, 0.0],
    })
    result = df.astype(bool)
    # All columns should be boolean dtype (numpy bool_ or pandas BooleanDtype)
    for dtype in result.dtypes:
        assert "bool" in str(dtype).lower()
    assert result["a"].sum() == 2
    assert result["b"].sum() == 2
    assert result["c"].sum() == 2


def test_mining_module_importable():
    """AssociationRuleMiner class is importable."""
    from ptn_analysis.analysis.mining import AssociationRuleMiner
    assert AssociationRuleMiner is not None
