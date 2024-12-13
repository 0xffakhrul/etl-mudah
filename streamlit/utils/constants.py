# Constants and configuration
MALAYSIA_STATES = {
    'Johor': {'lat': 1.4854, 'lon': 103.7618},
    'Kedah': {'lat': 6.1184, 'lon': 100.3685},
    'Kelantan': {'lat': 6.1254, 'lon': 102.2386},
    'Melaka': {'lat': 2.1896, 'lon': 102.2501},
    'Malacca': {'lat': 2.1896, 'lon': 102.2501},
    'Negeri Sembilan': {'lat': 2.7258, 'lon': 101.9424},
    'N. Sembilan': {'lat': 2.7258, 'lon': 101.9424},
    'Pahang': {'lat': 3.8126, 'lon': 103.3256},
    'Perak': {'lat': 4.5921, 'lon': 101.0901},
    'Perlis': {'lat': 6.4449, 'lon': 100.2048},
    'Pulau Pinang': {'lat': 5.4141, 'lon': 100.3288},
    'Penang': {'lat': 5.4141, 'lon': 100.3288},
    'P. Pinang': {'lat': 5.4141, 'lon': 100.3288},
    'Sabah': {'lat': 5.9788, 'lon': 116.0753},
    'Sarawak': {'lat': 1.5533, 'lon': 110.3592},
    'Selangor': {'lat': 3.0738, 'lon': 101.5183},
    'Terengganu': {'lat': 5.3117, 'lon': 103.1324},
    'Kuala Lumpur': {'lat': 3.1390, 'lon': 101.6869},
    'KL': {'lat': 3.1390, 'lon': 101.6869},
    'W.P. Kuala Lumpur': {'lat': 3.1390, 'lon': 101.6869},
    'Federal Territory of Kuala Lumpur': {'lat': 3.1390, 'lon': 101.6869}
}

def create_price_segment(price):
    if price < 25000:
        return 'Budget (< RM25k)'
    elif price < 50000:
        return 'Entry Level (RM25k-50k)'
    elif price < 100000:
        return 'Mid Range (RM50k-100k)'
    elif price < 200000:
        return 'Premium (RM100k-200k)'
    else:
        return 'Luxury (> RM200k)'

def create_motorcycle_price_segment(price):
    if price < 5000:
        return 'Budget (< RM5k)'
    elif price < 10000:
        return 'Entry Level (RM5k-10k)'
    elif price < 20000:
        return 'Mid Range (RM10k-20k)'
    elif price < 40000:
        return 'Premium (RM20k-40k)'
    else:
        return 'Luxury (> RM40k)' 