#!/usr/bin/env bash
# Clear pspcz_analyzer cache to force fresh data generation.
#
# Usage:
#   ./scripts/clear_cache.sh            # clear AI classifications only (fast re-generation)
#   ./scripts/clear_cache.sh --meta     # clear all tisky metadata (classifications, histories, diffs)
#   ./scripts/clear_cache.sh --parquet  # clear parsed voting/MP parquets (re-parse from UNL)
#   ./scripts/clear_cache.sh --text     # clear extracted text + classifications
#   ./scripts/clear_cache.sh --pdf      # clear PDFs + text + all metadata
#   ./scripts/clear_cache.sh --all      # nuke entire cache (re-download all data from psp.cz)

set -euo pipefail

CACHE_DIR="${HOME}/.cache/pspcz-analyzer/psp"

if [[ ! -d "$CACHE_DIR" ]]; then
    echo "Cache directory not found: $CACHE_DIR"
    exit 0
fi

MODE="${1:---topics}"

case "$MODE" in
    --topics|-t)
        echo "Clearing AI topic classifications and summaries..."
        find "$CACHE_DIR/tisky_meta" -name "topic_classifications.parquet" -delete 2>/dev/null || true
        echo "Done. Next server start will re-classify all tisky."
        echo "  - With Ollama running: free-form AI topics + Czech summaries"
        echo "  - Without Ollama: keyword-based topic classification, no summaries"
        ;;
    --meta|-m)
        echo "Clearing all tisky metadata..."
        echo "  This includes: AI classifications, histories, law changes,"
        echo "  subtisk versions, and version diffs."
        rm -rf "$CACHE_DIR/tisky_meta" 2>/dev/null || true
        echo "Done. Next server start will re-scrape and re-classify all tisky."
        ;;
    --parquet|-p)
        echo "Clearing parsed parquet data..."
        echo "  This includes: voting records, MP data, sessions, organs, etc."
        rm -rf "$CACHE_DIR/parquet" 2>/dev/null || true
        echo "Done. Next server start will re-parse from extracted UNL files."
        echo "  No re-downloading needed — raw ZIPs and extracted files are kept."
        ;;
    --text)
        echo "Clearing extracted text + all tisky metadata..."
        rm -rf "$CACHE_DIR/tisky_text" 2>/dev/null || true
        rm -rf "$CACHE_DIR/tisky_meta" 2>/dev/null || true
        echo "Done. Next server start will re-extract PDFs and re-classify."
        echo "  PDFs are kept — no re-downloading needed."
        ;;
    --pdf)
        echo "Clearing PDFs + extracted text + all tisky metadata..."
        rm -rf "$CACHE_DIR/tisky_pdf" 2>/dev/null || true
        rm -rf "$CACHE_DIR/tisky_text" 2>/dev/null || true
        rm -rf "$CACHE_DIR/tisky_meta" 2>/dev/null || true
        echo "Done. Next server start will re-download PDFs from psp.cz (slow)."
        ;;
    --all)
        echo "Nuking entire cache..."
        echo "  This will delete: $CACHE_DIR"
        echo ""
        du -sh "$CACHE_DIR" 2>/dev/null || true
        echo ""
        read -rp "Are you sure? [y/N] " confirm
        if [[ "$confirm" =~ ^[Yy]$ ]]; then
            rm -rf "$CACHE_DIR"
            echo "Done. Next server start will re-download everything from psp.cz."
        else
            echo "Aborted."
        fi
        ;;
    --status|-s)
        echo "Cache status: $CACHE_DIR"
        echo ""
        for dir in raw extracted parquet tisky_pdf tisky_text tisky_meta; do
            if [[ -d "$CACHE_DIR/$dir" ]]; then
                count=$(find "$CACHE_DIR/$dir" -type f 2>/dev/null | wc -l)
                size=$(du -sh "$CACHE_DIR/$dir" 2>/dev/null | cut -f1)
                printf "  %-15s %6s  (%d files)\n" "$dir/" "$size" "$count"
            else
                printf "  %-15s %6s\n" "$dir/" "(empty)"
            fi
        done
        echo ""
        du -sh "$CACHE_DIR" 2>/dev/null | awk '{print "  Total: " $1}'
        ;;
    --help|-h)
        echo "Usage: $0 [OPTION]"
        echo ""
        echo "Options:"
        echo "  --topics, -t   Clear AI classifications only (default)"
        echo "  --meta, -m     Clear all tisky metadata (classifications + histories + diffs)"
        echo "  --parquet, -p  Clear parsed voting/MP parquets (re-parse from UNL)"
        echo "  --text         Clear extracted text + all tisky metadata"
        echo "  --pdf          Clear PDFs + text + all tisky metadata"
        echo "  --all          Nuke entire cache (with confirmation)"
        echo "  --status, -s   Show cache disk usage"
        echo "  --help, -h     Show this help"
        ;;
    *)
        echo "Unknown option: $MODE"
        echo "Run '$0 --help' for usage."
        exit 1
        ;;
esac
