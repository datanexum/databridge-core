"""OCR image text extraction."""

from typing import Any, Dict, Optional


def ocr_image(
    file_path: str,
    language: str = "eng",
    tesseract_path: Optional[str] = None,
    max_chars: int = 5000,
) -> Dict[str, Any]:
    """Extract text from an image using OCR (Tesseract).

    Args:
        file_path: Path to the image file (PNG, JPG, etc.).
        language: Tesseract language code (default 'eng').
        tesseract_path: Optional path to tesseract executable.
        max_chars: Maximum characters to return.

    Returns:
        Dict with file, language, extracted text, and character count.

    Raises:
        ImportError: If pytesseract or Pillow is not installed.
    """
    try:
        import pytesseract
        from PIL import Image
    except ImportError:
        raise ImportError(
            "pytesseract/Pillow not installed. Run: pip install 'databridge-core[ocr]'"
        )

    if tesseract_path:
        pytesseract.pytesseract.tesseract_cmd = tesseract_path

    image = Image.open(file_path)
    text = pytesseract.image_to_string(image, lang=language)

    return {
        "file": file_path,
        "language": language,
        "text": text[:max_chars],
        "character_count": len(text),
    }
