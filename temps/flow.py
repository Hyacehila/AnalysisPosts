from pocketflow import Flow
from nodes import AnalyzeThemes, SentimentBasic,SentimentThinking

def ThemeAnalyzerFlow():

    # Create node instances
    ThemeAnalyzer = AnalyzeThemes(max_retries=3)
    
    # Connect nodes in sequence
    
    # Create flow starting with outline node
    flow = Flow(start=ThemeAnalyzer)
    
    return flow

def SentimentAnalyzerFlow():

    # Create node instances
    SentimentAnalyzerBasic = SentimentBasic(max_retries=3,wait=10)
    SentimentAnalyzerThinking = SentimentThinking(max_retries=3,wait=10)
    # Connect nodes in sequence
    SentimentAnalyzerBasic - "Continue" >> SentimentAnalyzerThinking
    # Create flow starting with outline node
    flow = Flow(start=SentimentAnalyzerBasic)
    
    return flow