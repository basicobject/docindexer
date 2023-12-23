import pdftotext
import re
import string

from repo import OpenSearchClient

opensearch_client = OpenSearchClient("localhost", 9200, ("admin", "admin"), "root-ca.pem")


# data types
class Document:
    def __init__(self, keyword: str, ref_id: int, w: int):
        self.keyword = keyword
        self.ref_id = ref_id
        self.w = w

    def __repr__(self):
        return f"({self.ref_id}, {self.keyword}, {self.w})"

    def to_dict(self):
        return {
            "ref_id": self.ref_id,
            "keyword": self.keyword,
            "w": self.w
        }


# util functions
def pdf_to_text(pdf_file_path: str) -> str:
    try:
        with open(pdf_file_path, "rb") as f:
            pdf = pdftotext.PDF(f)
            return "\n\n".join(pdf)
    except IOError:
        print(f"Extracting from pdf failed {pdf_file_path}")
        return ""


def download_from_s3(s3_url) -> str:
    return "local file path"


# Service layer
class TextTokenizer:
    def tokenize(self, text: str) -> list[str]:
        pass


# simple text split implementation
class SimpleTextTokenizer(TextTokenizer):
    def tokenize(self, text: str) -> list[str]:
        cleaned_text = self.cleanup(text)
        return cleaned_text.split(" ")

    def cleanup(self, text) -> str:
        def remove_non_ascii(a_str):
            return ''.join(
                filter(lambda x: x in string.printable, a_str)
            )

        step1 = remove_non_ascii(text)
        step2 = re.sub(r'[\r\n]+', '\n', step1)
        step3 = re.sub(r'\s+', " ", step2)  # remove duplicate spaces
        step4 = re.sub(r'\s+,', ",", step3)  # remove space before ,
        step5 = re.sub(r'\s+\.,', ".", step4)  # remove space before .
        step6 = re.sub(r' +', ' ', step5)  # remove duplicate spaces
        return step6


# advanced NLP based implementation
class AdvancedTextTokenizer(TextTokenizer):
    def tokenize(self, text: str) -> list[str]:
        return self.extract_tokens(text)

    def extract_tokens(self, text) -> list[str]:
        pass


class DocumentIndexer:
    tokenizer: TextTokenizer
    text_doc: str = ""
    keywords: set[str] = []
    documents: set[Document] = []
    ref: int

    def __init__(self, ref: int, pdfdoc=None, s3_url=None, tokenizer: TextTokenizer = SimpleTextTokenizer()):
        self.ref = ref
        self.tokenizer = tokenizer

        if pdfdoc is not None:
            self.text_doc = pdf_to_text(pdfdoc)
        elif s3_url is not None:
            local_file = download_from_s3(s3_url)
            self.text_doc = pdf_to_text(local_file)

    def index_document(self):
        token_with_freq = dict()
        tokens = self.tokenizer.tokenize(self.text_doc)

        for tok in tokens:
            token_with_freq[tok] = token_with_freq.get(tok, 0) + 1

        for k, v in token_with_freq.items():
            doc = Document(k, self.ref, v)
            self.documents.append(doc)

    def commit(self):  # write index to open search
        for i, doc in enumerate(self.documents):
            opensearch_client.write(i, doc.to_dict())


if __name__ == "__main__":
    indexer = DocumentIndexer(1, pdfdoc="Profile.pdf")
    indexer.index_document()
    indexer.commit()
