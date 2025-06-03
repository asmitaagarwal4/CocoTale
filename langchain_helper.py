from langchain_google_genai import GoogleGenerativeAIEmbeddings
import os
from pymongo import MongoClient
from langchain_mongodb import MongoDBAtlasVectorSearch
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_community.document_loaders.csv_loader import CSVLoader


load_dotenv()
# GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

os.environ.get("GOOGLE_API_KEY")
uri = os.getenv("MONGODB_ATLAS_URI")
# print(uri)


client = MongoClient(uri)
db = client["climate_ai"]
MONGODB_COLLECTION = db["climate_data"]
ATLAS_VECTOR_SEARCH_INDEX_NAME = "climate_data_index"

loader = CSVLoader(file_path="data/monthly_data.csv",content_columns="description",source_column="date",metadata_columns=["city_name","weather_summary"])
data = loader.load()

embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
# embedding = embeddings.embed_documents([data[0].page_content])
# print( len(embedding),len(embedding[0]))

vector_store = MongoDBAtlasVectorSearch(
    embedding=embeddings,
    collection=MONGODB_COLLECTION,
    index_name=ATLAS_VECTOR_SEARCH_INDEX_NAME,
    relevance_score_fn="cosine",
)

# doc_ids = vector_store.add_documents(data)
# print(f"Added {len(doc_ids)} documents to the vector store with IDs: {doc_ids}")
retriever = vector_store.as_retriever(search_kwargs={"k": 30})
query = "affect of climate change in jaipur over the years after 2000"
docs = retriever.invoke(query)
# print(docs)
prompt = f"""
you are an Ai story teller, you will be given a set of documents, and you will write a story based on the documents and the query.
Here are the documents:
{docs}  
Here is the query:
{query}
Write a story based on the documents and the query.
"""

model = init_chat_model("gemini-2.0-flash", model_provider="google_genai")
response = model.invoke(prompt)
print(response.content)
