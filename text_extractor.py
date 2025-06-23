# Author: Sebastian Orozco

import pandas as pd
import requests
import pymupdf  
import os
from io import BytesIO
import csv

class PDFTextExtractor:
    def __init__(self, csv_file, download_pdfs=True, download_dir="downloaded_pdfs", output_csv="extracted_texts.csv", output_txt="extracted_text.txt", output_dir=""):
        self.csv_file = csv_file
        self.download_pdfs = download_pdfs  # Toggle to download PDFs or process in memory
        self.download_dir = download_dir
        os.makedirs(self.download_dir, exist_ok=True)

        self.output_dir = output_dir
        if self.output_dir:
            os.makedirs(self.output_dir, exist_ok=True)

        # Set output file paths using output_dir if provided
        self.output_csv = os.path.join(self.output_dir, output_csv)
        self.output_txt = os.path.join(self.output_dir, output_txt)

    def download_pdf(self, url, save_path):
        # Downloads a PDF from a given URL and saves it locally
        try:
            response = requests.get(url, stream=True, headers={"User-Agent": "Mozilla/5.0"})
            if response.status_code == 200:
                with open(save_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=1024):
                        file.write(chunk)
                print(f"PDF downloaded: {save_path}")
                return save_path
            else:
                print(f"Failed to download {url} - HTTP {response.status_code}")
                return None
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return None

    def extract_text_from_pdf(self, pdf_source, is_file=False):
        # Extracts text from a PDF file or an in-memory stream
        try:
            if is_file:
                doc = pymupdf.open(pdf_source)  # Open saved PDF file
            else:
                doc = pymupdf.open(stream=pdf_source, filetype="pdf")  # Open PDF from memory

            text = "\n".join([page.get_text("text") for page in doc])
            return text.strip() if text else "No text found"
        except Exception as e:
            print(f"Error extracting text: {e}")
            return None
        
    def extract_text_from_pdf_url(self, pdf_url):
        # Downloads PDF from URL and extracts text
        try:
            response = requests.get(pdf_url, stream=True, headers={"User-Agent": "Mozilla/5.0"})
            if response.status_code == 200:
                pdf_data = BytesIO(response.content)
                return self.extract_text_from_pdf(pdf_data, is_file=False)
            else:
                print(f"Failed to fetch PDF: HTTP {response.status_code}")
                return None
        except Exception as e:
            print(f"Error fetching PDF: {e}")
            return None

    def process_csv(self):
        # Reads the CSV file, processes PDFs, and saves extracted text
        df = pd.read_csv(self.csv_file)

        if "attachmentLinks" not in df.columns:
            print("'attachmentLinks' column not found in CSV.")
            return

        # Open output files for writing
        with open(self.output_csv, "w", newline="", encoding="utf-8") as csvfile, open(self.output_txt, "w", encoding="utf-8") as txtfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(["commentId", "docketId", "pdf_text"])  # Write CSV headers

            for index, row in df.iterrows():
                comment_id = row["commentId"]
                docket_id = row["docketId"]
                pdf_urls = row["attachmentLinks"]

                if pd.notna(pdf_urls):
                    # Handle multiple URLs per row
                    pdf_urls = pdf_urls.split("|")
                    pdf_urls = [url.strip() for url in pdf_urls if url.strip().endswith(".pdf")]

                    if not pdf_urls:
                        print(f"No valid PDFs at index {index} with commentId {comment_id}")
                        continue

                    for pdf_url in pdf_urls:
                        print(f"Processing PDF from {pdf_url}...")

                        if self.download_pdfs:
                            # Save PDF to disk and extract text from file
                            pdf_filename = os.path.join(self.download_dir, f"pdf_{comment_id}.pdf")
                            pdf_path = self.download_pdf(pdf_url, pdf_filename)
                            if pdf_path:
                                pdf_text = self.extract_text_from_pdf(pdf_path, is_file=True)
                        else:
                            # Process PDF directly in memory
                            response = requests.get(pdf_url, stream=True, headers={"User-Agent": "Mozilla/5.0"})
                            if response.status_code == 200:
                                pdf_data = BytesIO(response.content)
                                pdf_text = self.extract_text_from_pdf(pdf_data, is_file=False)
                            else:
                                print(f"Failed to fetch {pdf_url} - HTTP {response.status_code}")
                                pdf_text = None

                        # Write extracted text to CSV and TXT
                        csv_writer.writerow([comment_id, docket_id, pdf_text])
                        txtfile.write(f"\n--- COMMENT ID: {comment_id} AND DOCKET ID: {docket_id} ---\n{pdf_text}\n")
                        print(f"Extracted text for commentId {comment_id} written to CSV & TXT.")

        print(f"All extracted text saved to {self.output_csv} & {self.output_txt}")


# if __name__ == "__main__":
#     extractor = PDFTextExtractor(csv_file="temp.csv", download_pdfs=False)
#     extractor.process_csv()