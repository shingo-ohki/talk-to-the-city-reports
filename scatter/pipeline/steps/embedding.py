
from langchain.embeddings import OpenAIEmbeddings
import pandas as pd
from tqdm import tqdm

from utils import local_llm_embedding

def embedding(config):
    dataset = config['output_dir']
    path = f"outputs/{dataset}/embeddings.pkl"
    arguments = pd.read_csv(f"outputs/{dataset}/args.csv")
    embeddings = []
    model = config['embedding'].get('model', 'gpt3.5-turbo')
    for i in tqdm(range(0, len(arguments), 1000)):
        args = arguments["argument"].tolist()[i: i + 1000]
        if model.startswith("local:"):
            embeds = [local_llm_embedding(arg, model) for arg in args]
        else:
            embeds = OpenAIEmbeddings().embed_documents(args)
        embeddings.extend(embeds)
    df = pd.DataFrame(
        [
            {"arg-id": arguments.iloc[i]["arg-id"], "embedding": e}
            for i, e in enumerate(embeddings)
        ]
    )
    df.to_pickle(path)
