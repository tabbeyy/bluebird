from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyser = SentimentIntensityAnalyzer()

def naive_weight(text: str, terms: list[str]) -> int:
    """
    Compute the weight of the provided words from `terms` in `text`
    """
    term_counts = {t:0 for t.upper() in terms}    
    
    for i, word in enumerate(text.split()):
        if word.upper() in term_counts:
            term_counts[word.upper()] += 1
    
    return sum(
        [term_counts[i]/=i+1 for i in term_counts])/len(x)

def vader(text: str) -> list[int]:
    """
    VADER sentiment analysis of a string `text`.
    Returns a list 3 floats corresponding to the text's scores on positivity,
    neutrality, and negativity.   
    """
    scores = analyser.polarity_scores(text)
    return [scores["pos"], scores["neu"], scores["neg"]]
    
