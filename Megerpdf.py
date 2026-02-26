import os
import sqlite3
from PIL import Image, ImageFile
from pypdf import PdfMerger
import tempfile

# ==============================
# 🔹 CONFIG
# ==============================

SQLITE_PATH = r"C:\Users\az72987\Desktop\minejendom2filarkiv.db"
OUTPUT_PDF = r"C:\Users\az72987\Desktop\MERGED_81792653.pdf"
FILARKIV_ID = ## indsæt her

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True


# ==============================
# 🔹 Convert image to temp PDF
# ==============================
def convert_image_to_pdf(image_path):
    image = Image.open(image_path)

    if image.mode in ("RGBA", "P"):
        image = image.convert("RGB")

    temp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    image.save(temp_pdf.name, "PDF", resolution=300.0)

    return temp_pdf.name


# ==============================
# 🔹 Main logic
# ==============================
def main():

    print("Connecting to SQLite...")
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()

    query = """
    SELECT Id,
           CaseId,
           DocumentDate,
           Title,
           FileName,
           FilePath,
           FileExtension,
           PageNumber,
           DocumentType,
           DocumentTypeName,
           FilArkivDocumentId,
           FilArkivFileId,
           IsScannedPage,
           Processed,
           ParentReference,
           MergedDocumentId,
           Note,
           UploadedAt
    FROM MinEjendom_Documents
    WHERE FilArkivDocumentId LIKE ?
    ORDER BY PageNumber, Id;
    """

    print("Executing query...")
    cur.execute(query, (FILARKIV_ID,))
    rows = cur.fetchall()

    if not rows:
        print("No documents found.")
        return

    print(f"Found {len(rows)} pages")

    merger = PdfMerger()
    temp_files = []

    for row in rows:
        file_path = row[5]
        extension = (row[6] or "").lower()
        page_number = row[7]

        if not file_path or not os.path.exists(file_path):
            print(f"⚠ Missing file (Page {page_number}): {file_path}")
            continue

        try:
            if extension in ["jpg", "jpeg", "tif", "tiff", "png"]:
                print(f"Converting Page {page_number}: {file_path}")
                pdf_path = convert_image_to_pdf(file_path)
                temp_files.append(pdf_path)
                merger.append(pdf_path)

            elif extension == "pdf":
                print(f"Adding PDF Page {page_number}: {file_path}")
                merger.append(file_path)

            else:
                print(f"Skipping unsupported file type: {file_path}")

        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")

    print("Writing final merged PDF...")
    merger.write(OUTPUT_PDF)
    merger.close()

    # Cleanup temp files
    for f in temp_files:
        try:
            os.remove(f)
        except:
            pass

    conn.close()

    print("\n✅ DONE")
    print(f"Final merged PDF created at:\n{OUTPUT_PDF}")


if __name__ == "__main__":
    main()