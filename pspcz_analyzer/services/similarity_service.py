"""Voting similarity analysis with PCA clustering."""

import numpy as np
import polars as pl

from pspcz_analyzer.models.enums import VoteResult
from pspcz_analyzer.services.data_service import PeriodData


def _build_vote_matrix(data: PeriodData) -> tuple[np.ndarray, pl.DataFrame]:
    """Build a vote matrix (MPs x votes).

    Values: +1 = YES, -1 = NO, 0 = anything else.
    Returns the matrix and the MP info DataFrame (aligned by row index).
    """
    # Exclude void votes
    void_ids = data.void_votes.get_column("id_hlasovani")
    mp_votes = data.mp_votes.filter(~pl.col("id_hlasovani").is_in(void_ids))

    # Encode votes as numeric
    mp_votes = mp_votes.with_columns(
        pl.when(pl.col("vysledek") == VoteResult.YES)
        .then(1)
        .when(pl.col("vysledek") == VoteResult.NO)
        .then(-1)
        .otherwise(0)
        .alias("vote_num")
    )

    # Pivot: rows=MPs, cols=votes
    pivot = mp_votes.pivot(
        on="id_hlasovani",
        index="id_poslanec",
        values="vote_num",
        aggregate_function="first",
    ).fill_null(0)

    mp_ids = pivot.select("id_poslanec")
    matrix = pivot.drop("id_poslanec").to_numpy().astype(np.float32)

    # Align MP info
    mp_info = mp_ids.join(data.mp_info, on="id_poslanec", how="left")

    return matrix, mp_info


def compute_pca_coords(data: PeriodData) -> list[dict]:
    """Compute 2D PCA coordinates for each MP based on voting patterns.

    Returns list of dicts with: mp_name, party, x, y
    """
    matrix, mp_info = _build_vote_matrix(data)

    # Center the data
    matrix_centered = matrix - matrix.mean(axis=0)

    # SVD-based PCA (no sklearn needed)
    U, S, Vt = np.linalg.svd(matrix_centered, full_matrices=False)
    coords_2d = U[:, :2] * S[:2]

    names = (mp_info.get_column("jmeno") + " " + mp_info.get_column("prijmeni")).to_list()
    parties = mp_info.get_column("party").to_list()

    return [
        {
            "mp_name": names[i],
            "party": parties[i] or "N/A",
            "x": float(coords_2d[i, 0]),
            "y": float(coords_2d[i, 1]),
        }
        for i in range(len(names))
    ]


def compute_cross_party_similarity(data: PeriodData, top: int = 20) -> list[dict]:
    """Find the most similar cross-party MP pairs.

    Uses cosine similarity on the vote matrix.
    """
    matrix, mp_info = _build_vote_matrix(data)

    # Compute cosine similarity
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1, norms)  # avoid division by zero
    normalized = matrix / norms
    similarity = normalized @ normalized.T

    names = (mp_info.get_column("jmeno") + " " + mp_info.get_column("prijmeni")).to_list()
    parties = mp_info.get_column("party").to_list()

    # Find top cross-party pairs
    n = len(names)
    pairs = []
    for i in range(n):
        for j in range(i + 1, n):
            if parties[i] and parties[j] and parties[i] != parties[j]:
                pairs.append(
                    {
                        "mp1_name": names[i],
                        "mp1_party": parties[i],
                        "mp2_name": names[j],
                        "mp2_party": parties[j],
                        "similarity": float(similarity[i, j]),
                    }
                )

    pairs.sort(key=lambda p: p["similarity"], reverse=True)
    return pairs[:top]
