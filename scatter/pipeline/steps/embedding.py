from transformers import AutoTokenizer, AutoModel
import torch
import pandas as pd
from tqdm import tqdm

# LINE DistilBERTのモデルとトークナイザーを初期化
tokenizer = AutoTokenizer.from_pretrained("line-corporation/line-distilbert-base-japanese", trust_remote_code=True)
model = AutoModel.from_pretrained("line-corporation/line-distilbert-base-japanese")
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

class LineDistilBERTEmbeddings:
    def embed_documents(self, texts):
        inputs = tokenizer(texts, padding=True, truncation=True, return_tensors="pt").to(device)
        with torch.no_grad():
            outputs = model(**inputs)
        embeddings = outputs.last_hidden_state[:, 0, :].cpu().numpy()
        return embeddings.tolist()

def embedding(config):
    dataset = config['output_dir']
    path = f"outputs/{dataset}/embeddings.pkl"
    arguments = pd.read_csv(f"outputs/{dataset}/args.csv")
    embeddings = []
    for i in tqdm(range(0, len(arguments), 1000)):
        args = arguments["argument"].tolist()[i: i + 1000]
        embeds = LineDistilBERTEmbeddings().embed_documents(args)
        embeddings.extend(embeds)
    df = pd.DataFrame(
        [
            {"arg-id": arguments.iloc[i]["arg-id"], "embedding": e}
            for i, e in enumerate(embeddings)
        ]
    )
    df.to_pickle(path)
