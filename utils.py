from pathlib import Path

def load_keys():
    """Load Alpaca keys from keys.env file"""
    env_file = Path(__file__).parent / "keys.env"
    
    if not env_file.exists():
        raise FileNotFoundError("keys.env file not found!")
    
    keys = {}
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                keys[key.strip()] = value.strip()
    
    return keys.get('ALPACA_API_KEY'), keys.get('ALPACA_SECRET_KEY')


def load_all_keys():
    """Load all keys including Discord"""
    env_file = Path(__file__).parent / "keys.env"
    
    if not env_file.exists():
        raise FileNotFoundError("keys.env file not found!")
    
    keys = {}
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                keys[key.strip()] = value.strip()
    
    return keys