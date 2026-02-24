"""PDF text extraction."""

from typing import Any, Dict, List


def extract_pdf_text(
    file_path: str,
    pages: str = "all",
    max_chars_per_page: int = 2000,
) -> Dict[str, Any]:
    """Extract text content from a PDF file.

    Args:
        file_path: Path to the PDF file.
        pages: Page selection ('all', '1,2,3', or '1-5').
        max_chars_per_page: Maximum characters to extract per page.

    Returns:
        Dict with file info, page count, and extracted content.

    Raises:
        ImportError: If pypdf is not installed.
    """
    try:
        from pypdf import PdfReader
    except ImportError:
        raise ImportError(
            "pypdf not installed. Run: pip install 'databridge-core[pdf]'"
        )

    reader = PdfReader(file_path)
    total_pages = len(reader.pages)

    if pages == "all":
        page_nums = list(range(total_pages))
    elif "-" in pages:
        start, end = map(int, pages.split("-"))
        page_nums = list(range(start - 1, min(end, total_pages)))
    else:
        page_nums = [int(p.strip()) - 1 for p in pages.split(",")]

    extracted = []
    for page_num in page_nums:
        if 0 <= page_num < total_pages:
            text = reader.pages[page_num].extract_text() or ""
            extracted.append({
                "page": page_num + 1,
                "text": text[:max_chars_per_page],
            })

    return {
        "file": file_path,
        "total_pages": total_pages,
        "pages_extracted": len(extracted),
        "content": extracted,
    }
