import os
from pymongo import MongoClient
import csv
from langchain.chat_models import init_chat_model
# from langchain_community.prompts import PromptTemplate
from langchain_core.prompts import PromptTemplate



GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
MONGODB_ATLAS_URI = os.getenv("MONGODB_ATLAS_URI")  

client = MongoClient(MONGODB_ATLAS_URI)
db = client["climate_ai"]
collection = db["climate_summary"]

from pymongo import MongoClient

client = MongoClient(MONGODB_ATLAS_URI)


csvfile = "data/monthly_data.csv"

def add_data_to_mongodb():
    try:
        client.admin.command('ping')  # Test the connection
        print("Connected to MongoDB successfully.")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")

    if collection.count_documents({}) > 0:
        print("Data already exists in the 'climate_summary' collection. Skipping insertion.")
        return
    
    with open(csvfile, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = list(reader)
        if data:
            collection.insert_many(data)

# add_data_to_mongodb()


def extract_metadata(query):
    # Initialize the Gemini model
    model = init_chat_model("gemini-2.0-flash", model_provider="google_genai")

    # Define a prompt for metadata extraction
    metadata_prompt = PromptTemplate(
        input_variables=["query"],
        template="""
        Extract the following metadata from the query:
        - city_name (capitalized city name, e.g., "Delhi")
        - Start date  (date range in the format yyyy-mm)
        - end date (date range in the format yyyy-mm)
        -season (if mentioned)

        min start date is 1990-01
        maxm end date is 2020-12 
        add these if not given in the query
        Query: {query}
        Metadata:
        """
    )

    # Generate metadata
    prompt = metadata_prompt.format(query=query)
    response = model.invoke(prompt)

    # print(response)

    # Parse the response into a dictionary
    metadata = {}
    for line in response.content.split("\n"):
        if ": " in line:
            key, value = line.split(": ", 1)
            # Remove extra quotes and commas
            key = key.strip().strip('"')
            value = value.strip().strip('",')
            metadata[key] = value
    print(f"Extracted Metadata: {metadata}")
    return metadata 


# extract_metadata("How has the temperature in Surat changed over the years after 2000?")
def retrive(metadata):

    # Build the metadata filter
    metadata_filter = {}
    if "city_name" in metadata:
        metadata_filter["city_name"] = metadata["city_name"]
    if "start_date" and "end_date" in metadata:
        # Normalize the source field to ensure correct format
        metadata_filter["date"] = {"$gte": metadata["start_date"], "$lte": metadata["end_date"]}

    # print(f"Applying metadata filter: {metadata_filter}")

    # Debugging: Print all documents in the collection
    # all_docs = list(collection.find())
    # print(f"All Documents in Collection: {all_docs}")

    # Apply metadata filtering
    filtered_docs = list(collection.find(metadata_filter))
    print(f"Filtered {len(filtered_docs)} documents based on metadata")
    return filtered_docs

def generate(query):
    # Extract metadata from the query
    metadata = extract_metadata(query)
    docs = retrive(metadata)
    
    # 1. Analyze the provided documents and extract relevant information.
    # 3. Highlight any significant extreme weather events or years that are relevant to the query.
    # 4. if no data is available for the query, just mention that explicitly
    # 5. Write a cohesive and engaging story that answers the query while incorporating the key details from the documents.
    # 6. Ensure the story is easy to read and provides meaningful insights.

    prompt = f"""
    You are an AI storyteller. Your task is to write a compelling and informative story based on the provided documents and query.

    ### Documents:
    {docs}

    ### Query:
    {query}

    ### Instructions:
    1. Imagine you are narrating this story to an audience. Use a conversational and descriptive tone.
    2. Highlight key events, patterns, and notable years in a way that feels like storytelling, not just listing facts.
    3. Use vivid language to describe the weather and its impact on people, nature, and the environment.
    5. If no data is available for the query, mention it explicitly in a graceful way.
    6. Ensure the story flows naturally and is engaging to read.
    

    ### Output:
    Write the story below:
    """
    
    # Initialize the Gemini model
    model = init_chat_model("gemini-2.0-flash", model_provider="google_genai")
    
    # Generate the response
    response = model.invoke(prompt)
    
    return response.content


query = "How has the temperature in ahmedabad changed over the years after 2000 in summers?"
# metadata = extract_metadata(query)
# docs = retrive(metadata)
# print(docs)
print(generate(query))