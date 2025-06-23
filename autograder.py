# Author: Sebastian Orozco

from openai import OpenAI
from text_extractor import PDFTextExtractor
import pandas as pd
import csv
from secrets import OPEN_API_KEY

total_tokens = 0

client = OpenAI(
  api_key=OPEN_API_KEY
)

def grade_comment(comment_id, comment_text):
    prompt = f"""
    You are a grading assistant for Public Participation in Environmental Regulation (PPER).
    Use the criteria below to grade the given comment.

    **INSTRUCTIONS:**
    - Use the categories and scales provided to objectively assess the comment.
    
    **Criteria:**
    1. **Authorship:** Citizen, NGO/Activist Group, Business/Industry Representative, Government/Agency Official, Other.
    2. **Professionalism (1-5):** 1 = Informal, disrespectful, 5 = Formal, respectful.
    3. **Use of Evidence/Reasoning:** Yes or No.
    4. **Logical Argumentation (1-5):** 1 = No logical sense, 5 = Very compelling, clear logic.
    5. **Emotional Tone:** Positive, Neutral, Negative.
    6. **Pro-Environmental Regulation (1-5):** 1 = Strongly anti-environment, 5 = Strongly pro-environment.
    7. **Bias:** Primarily economic concerns, Primarily environmental concerns, Balanced perspective, Other.
    8. **Other Feedback:** Any additional insights.

    Please provide the grading in the following format:

    Authorship: <Answer>  
    Professionalism: <Answer>  
    Use of Evidence/Reasoning: <Answer>  
    Logical Argumentation: <Answer>  
    Emotional Tone: <Answer>  
    Pro-Environmental Regulation: <Answer>  
    Bias: <Answer>  
    Other Feedback: <Answer>
    """

    # Send the prompt to OpenAI's API
    completion = client.chat.completions.create(
    model="gpt-4o-mini",
    store=True,
    messages=[
        {"role": "developer", "content": prompt},
        {"role": "user", "content": f"Please grade this comment with id: {comment_id} and content: {comment_text}"}
    ]
    )

    global total_tokens 
    total_tokens += completion.usage.total_tokens

    return completion.choices[0].message.content

# Processes a CSV of PDF links, grades them, and writes to a separate CSV.
def grade_all_comments(input_csv, output_csv):
    
    # Read input CSV
    df = pd.read_csv(input_csv)
    extractor = PDFTextExtractor(csv_file=input_csv, download_pdfs=False)

    if "commentId" not in df.columns or "attachmentLinks" not in df.columns:
        print("Required columns 'commentId' or 'attachmentLinks' not found in CSV.")
        return

    # Process each PDF and write graded feedback to output CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["commentId", "graded_feedback"])  # CSV headers

        for index, row in df.iterrows():
            comment_id = row["commentId"]
            pdf_links = row["attachmentLinks"]

            if pd.notna(pdf_links):
                pdf_urls = [url.strip() for url in pdf_links.split("|") if url.strip().endswith(".pdf")]

                for pdf_url in pdf_urls:
                    print(f"Processing PDF from {pdf_url}...")

                    # Extract text from the PDF
                    pdf_text = extractor.extract_text_from_pdf_url(pdf_url)

                    if pdf_text:
                        # Grade the comment using existing function
                        graded_feedback = grade_comment(comment_id, pdf_text)
                        csv_writer.writerow([comment_id, graded_feedback])
                        print(f"Graded feedback for commentId {comment_id} written to output CSV.")
                    else:
                        print(f"No text extracted for commentId {comment_id} from {pdf_url}")

    print(f"All graded feedback saved to {output_csv}")
    print(f"Total tokens used: {total_tokens}")


# Example usage
grade_all_comments("data/EPA_Comments.csv", "data/graded_feedback.csv")


# For debugging
# pdf_url = "https://downloads.regulations.gov/EPA-R03-RCRA-2008-0256-0005/attachment_1.pdf"
# comment_id = pdf_url.split('.gov/')[1].split('/')[0]

# extractor = PDFTextExtractor(csv_file="data/EPA_Comments.csv", download_pdfs=False)
# comment_text = extractor.extract_text_from_pdf_url(pdf_url)

# print(comment_text)

# comment_id = "EPA-R03-RCRA-2008-0256"
# comment_text = "I don't support this regulation because I don't like the environment."

# graded_feedback = grade_comment(comment_id, comment_text)
# print(graded_feedback)
