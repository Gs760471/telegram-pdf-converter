import os
import pdfplumber
import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, ContextTypes, filters

TOKEN = os.getenv("BOT_TOKEN")  # set in Render env vars

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Send me a PDF, I’ll convert it to Excel 😊")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document

    if not doc.file_name.lower().endswith(".pdf"):
        await update.message.reply_text("Please send a PDF file.")
        return

    await update.message.reply_text("Got your PDF, converting to Excel. Please wait...")

    file = await doc.get_file()
    pdf_path = f"/tmp/{doc.file_name}"
    xlsx_path = pdf_path.replace(".pdf", ".xlsx")

    # download PDF
    await file.download_to_drive(pdf_path)

    # convert PDF → Excel (simple table extraction)
    all_rows = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                for row in table:
                    all_rows.append(row)

    if not all_rows:
        await update.message.reply_text("Sorry, I couldn’t detect any tables in this PDF.")
        return

    df = pd.DataFrame(all_rows)
    df.to_excel(xlsx_path, index=False)

    # send back Excel file
    await update.message.reply_document(document=open(xlsx_path, "rb"),
                                        filename=os.path.basename(xlsx_path),
                                        caption="Here is your Excel file ✅")

def main():
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    app.run_polling()

if __name__ == "__main__":
    main()
