import os
import json
import glob
import time

class TopicGeneratorWorker:
    def __init__(self):
        pass

    def process(self, request_file):
        print(f"Processing request: {request_file}")
        # Validate or prepare context
        return True

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    GENERATED_DIR = os.path.join(BASE_DIR, "data", "topic_generated")
    
    worker = TopicGeneratorWorker()
    
    # Process all pending requests
    files = glob.glob(f"{GENERATED_DIR}/request_*.json")
    if not files:
        print("No generation requests found.")
        exit(0)
        
    for fpath in files:
        worker.process(fpath)
        # In a real system, might move to a 'processing' folder
        # For now, just leave it there or update status
        
    print(f"Processed {len(files)} generation requests.")