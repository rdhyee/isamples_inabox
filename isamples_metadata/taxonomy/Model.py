import torch
from transformers import BertTokenizer, BertForSequenceClassification


class Model:
    def __init__(self, config):
        self.config = config
        self.device = torch.device(
            "cuda:0" if torch.cuda.is_available() else "cpu"
        )
        self.tokenizer = BertTokenizer.from_pretrained(self.config["BERT_MODEL"])
        classifier = BertForSequenceClassification.from_pretrained(
            self.config["FINE_TUNED_MODEL"],
            num_labels=len(self.config["CLASS_NAMES"])
        )
        classifier = classifier.eval()
        self.classifier = classifier.to(self.device)

    def predict(self, text):
        encoded_text = self.tokenizer.encode_plus(
            text,
            max_length=self.config["MAX_SEQUENCE_LEN"],
            add_special_tokens=True,
            padding='max_length',
            truncation=True,
            return_tensors="pt",
        )
        input_ids = encoded_text["input_ids"].to(self.device)
        attention_mask = encoded_text["attention_mask"].to(self.device)
        with torch.no_grad():
            logits = self.classifier(input_ids, attention_mask)[0]
        # convert logits into probability
        prob = logits.softmax(dim=-1)[0]
        # get the top 3 high confidence predictions
        top_probs, indices = torch.topk(prob, 3)
        # convert integer labels to text labels
        indices = [x.item() for x in indices]
        labels = [self.config["CLASS_NAMES"][x] for x in indices]
        # convert torch tensor to float type
        probs = [x.item() for x in top_probs]
        return [(label, prob) for label, prob in zip(labels, probs)]
