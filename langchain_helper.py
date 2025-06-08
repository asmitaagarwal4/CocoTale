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

csvfile = "data/monthly_data.csv"

embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
# embedding = embeddings.embed_documents([data[0].page_content])
# print( len(embedding),len(embedding[0]))

vector_store = MongoDBAtlasVectorSearch(
    embedding=embeddings,
    collection=MONGODB_COLLECTION,
    index_name=ATLAS_VECTOR_SEARCH_INDEX_NAME,
    relevance_score_fn="cosine",
)

def create_Index():
    # loader = CSVLoader(file_path=csvfile,content_columns="description",source_column="date",metadata_columns=["city_name","weather_summary"])
    # data = loader.load()

    # doc_ids = vector_store.add_documents(data)
    # print(f"Added {len(doc_ids)} documents to the vector store")
    vec = embeddings.embed_query("change in temeperatue in Surat over the years after 2000")
    with open("vector_output.txt", "w") as file:
        file.write(f"Vector Length: {len(vec)}\n")
        file.write(f"Vector: {vec}")
    print("Vector written to vector_output.txt")



def generate(query):
    retriever = vector_store.as_retriever(search_kwargs={"k": 30})
    
    docs = retriever.invoke(query)
    retrieved_docs = vector_store.similarity_search(query = query, k=30) 
    print(retrieved_docs)
    # print(query)
    prompt = f"""
    you are an Ai story teller, you will be given a set of documents, and you will write a story based on the documents and the query.
    Here are the documents:
    {docs}  
    Here is the query:
    {query}
    Write a story based on the documents and the query.
    """

    # model = init_chat_model("gemini-2.0-flash", model_provider="google_genai")
    # response = model.invoke(prompt)
    # return response

query = "give me information about Surat"
print(generate(query))

# create_Index()

