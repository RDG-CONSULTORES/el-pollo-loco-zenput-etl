#!/usr/bin/env python3
"""
Debug supervisiones de seguridad
"""
import requests
import json

# Configuración Zenput
ZENPUT_CONFIG = {
    'base_url': 'https://www.zenput.com/api/v3',
    'headers': {'X-API-TOKEN': 'cb908e0d4e0f5501c635325c611db314'}
}

def debug_seguridad():
    """Debug submissions de seguridad"""
    
    print("🔍 Debug supervisiones seguridad...")
    
    url = f"{ZENPUT_CONFIG['base_url']}/submissions"
    params = {
        'form_template_id': '877139',
        'limit': 3,
        'created_after': '2025-01-01'
    }
    
    try:
        response = requests.get(url, headers=ZENPUT_CONFIG['headers'], params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            submissions = data.get('data', [])
            
            print(f"✅ Found {len(submissions)} submissions")
            
            for i, submission in enumerate(submissions):
                print(f"\n--- SUBMISSION {i+1} ---")
                print(f"Type: {type(submission)}")
                print(f"Is None: {submission is None}")
                
                if submission:
                    print(f"Keys: {list(submission.keys())}")
                    print(f"ID: {submission.get('id')}")
                    print(f"Has smetadata: {'smetadata' in submission}")
                    
                    if 'smetadata' in submission:
                        smetadata = submission['smetadata']
                        print(f"smetadata type: {type(smetadata)}")
                        if smetadata:
                            print(f"smetadata keys: {list(smetadata.keys())}")
                else:
                    print("❌ Submission is None!")
        else:
            print(f"❌ Error API: {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_seguridad()