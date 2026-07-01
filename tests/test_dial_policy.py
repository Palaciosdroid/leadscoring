"""Tests for the shared positive push policy (scoring.dial_policy)."""
import pytest

from scoring.dial_policy import should_push_lead, SCORE_WARM, FRESH_MIN_SCORE


class TestShouldPushLead:
    def test_eignungscheck_always_pushes_regardless_of_score(self):
        assert should_push_lead(score=0, is_fresh=False, list_key="eignungscheck") is True

    def test_warm_score_pushes(self):
        assert should_push_lead(score=SCORE_WARM, is_fresh=False, list_key="hypnose_warm") is True

    def test_below_warm_and_not_fresh_does_not_push(self):
        assert should_push_lead(score=SCORE_WARM - 1, is_fresh=False, list_key="hypnose_warm") is False

    def test_fresh_needs_min_score_floor(self):
        # fresh but below FRESH_MIN_SCORE → no push (the bug realtime used to have)
        assert should_push_lead(score=FRESH_MIN_SCORE - 1, is_fresh=True, list_key="hypnose_fresh") is False

    def test_fresh_at_floor_pushes(self):
        assert should_push_lead(score=FRESH_MIN_SCORE, is_fresh=True, list_key="hypnose_fresh") is True

    def test_dormant_warm_pushes_even_when_cold_now(self):
        assert should_push_lead(score=0, is_fresh=False, list_key=None, is_dormant_warm=True) is True

    def test_cold_no_list_no_push(self):
        assert should_push_lead(score=5, is_fresh=False, list_key=None) is False

    def test_returns_bool_not_truthy(self):
        # callers do `has_phone and should_push_lead(...)` — must be a real bool
        assert should_push_lead(score=100, is_fresh=False, list_key=None) is True


class TestConsistency:
    def test_scorer_uses_the_same_thresholds(self):
        # scorer must import the single-source constants (no drift)
        from batch import scorer
        assert scorer.SCORE_WARM == SCORE_WARM
        assert scorer.FRESH_MIN_SCORE == FRESH_MIN_SCORE

    def test_main_uses_the_shared_gate(self):
        # all three push paths reference the same function object
        import main
        from batch import scorer
        assert main.should_push_lead is should_push_lead
        assert scorer.should_push_lead is should_push_lead
