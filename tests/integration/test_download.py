"""Integration tests: real downloads from psp.cz."""

import pytest

pytestmark = pytest.mark.integration


class TestDownloads:
    def test_voting_data_downloads(self, voting_dir_period1):
        """Period 1 voting data should download and extract successfully."""
        assert voting_dir_period1.exists()
        assert voting_dir_period1.is_dir()

    def test_voting_dir_contains_unl_files(self, voting_dir_period1):
        """Extracted voting dir should contain .unl files."""
        unl_files = list(voting_dir_period1.rglob("*.unl"))
        assert len(unl_files) > 0

    def test_voting_dir_has_summary_file(self, voting_dir_period1):
        """Should contain the voting summary file hl1993s.unl."""
        matches = list(voting_dir_period1.rglob("hl1993s.unl"))
        assert len(matches) == 1

    def test_voting_dir_has_mp_vote_files(self, voting_dir_period1):
        """Should contain individual MP vote files (hl1993h*.unl)."""
        matches = list(voting_dir_period1.rglob("hl1993h*.unl"))
        assert len(matches) > 0

    def test_poslanci_downloads(self, poslanci_dir):
        """MP data should download and extract successfully."""
        assert poslanci_dir.exists()
        # Check for key files
        osoby = list(poslanci_dir.rglob("osoby.unl"))
        assert len(osoby) == 1
        poslanec = list(poslanci_dir.rglob("poslanec.unl"))
        assert len(poslanec) == 1

    def test_schuze_downloads(self, schuze_dir):
        """Session data should download and extract successfully."""
        assert schuze_dir.exists()
        schuze = list(schuze_dir.rglob("schuze.unl"))
        assert len(schuze) == 1
