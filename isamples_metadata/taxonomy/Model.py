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

        prob, predicted_class = torch.max(logits, dim=1)

        predicted_class = predicted_class.cpu().item()
        prob = logits.softmax(dim=-1)[0][predicted_class].item()
        return (
            self.config["CLASS_NAMES"][predicted_class],
            prob
        )
