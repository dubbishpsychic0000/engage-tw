import asyncio
import os
import hashlib
import json
import pickle
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import random
import time
import logging
import re

# Import twscrape - latest version
from twscrape import API, gather, Tweet, User
from twscrape.logger import set_log_level

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Fetch credentials from .env - Only cookies needed now
TWITTER_COOKIES = os.getenv("TWITTER_COOKIES", "")

# Global API instance
api = None


class TwitterScraperError(Exception):
    """Exception personnalisée pour le scraper Twitter"""
    pass


def setup_driver() -> bool:
    """Initialize twscrape API instance with anti-detection options."""
    global api
    try:
        logger.info("Initializing twscrape API...")

        # Initialize API with accounts database
        api = API("accounts.db")

        # Set debug level for troubleshooting
        set_log_level("INFO")  # Reduced logging for cleaner output

        logger.info("twscrape API initialized successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to initialize twscrape API: {e}")
        return False


def parse_cookies_string(cookies_string: str) -> Dict[str, str]:
    """Parse cookies string into dictionary format."""
    cookies_dict = {}
    if not cookies_string:
        return cookies_dict

    # Handle different cookie formats
    cookie_pairs = cookies_string.split(';')

    for pair in cookie_pairs:
        pair = pair.strip()
        if '=' in pair:
            key, value = pair.split('=', 1)
            cookies_dict[key.strip()] = value.strip()

    return cookies_dict


def validate_cookies_format(cookies_dict: Dict[str, str]) -> Tuple[bool, List[str]]:
    """Validate that essential cookies are present and properly formatted."""
    essential_cookies = {
        'auth_token': 'Authentication token',
        'ct0': 'CSRF token', 
        'guest_id': 'Guest identifier'
    }

    missing_cookies = []
    for cookie, description in essential_cookies.items():
        if cookie not in cookies_dict or not cookies_dict[cookie]:
            missing_cookies.append(f"{cookie} ({description})")

    # Additional validation for cookie values
    if 'auth_token' in cookies_dict:
        auth_token = cookies_dict['auth_token']
        if len(auth_token) < 40 or not re.match(r'^[a-f0-9]+$', auth_token):
            logger.warning("auth_token format may be invalid")

    if 'ct0' in cookies_dict:
        ct0 = cookies_dict['ct0']
        if len(ct0) < 32 or not re.match(r'^[a-f0-9]+$', ct0):
            logger.warning("ct0 format may be invalid")

    return len(missing_cookies) == 0, missing_cookies


def validate_credentials() -> bool:
    """Valide que les cookies sont présents et bien formatés"""
    if not TWITTER_COOKIES:
        logger.error("TWITTER_COOKIES est requis dans le fichier .env")
        logger.info("Pour obtenir vos cookies:")
        logger.info("1. Connectez-vous à twitter.com dans votre navigateur")
        logger.info("2. F12 → Application/Storage → Cookies → twitter.com")
        logger.info("3. Copiez tous les cookies et ajoutez-les dans TWITTER_COOKIES")
        logger.info("Format: auth_token=xxx; ct0=yyy; guest_id=zzz; ...")
        return False

    # Parse and validate cookies
    cookies_dict = parse_cookies_string(TWITTER_COOKIES)
    is_valid, missing_cookies = validate_cookies_format(cookies_dict)

    if not is_valid:
        logger.error(f"Cookies manquants ou invalides: {', '.join(missing_cookies)}")
        return False

    logger.info("✓ Cookies validés avec succès")
    return True


async def add_account_with_cookies() -> bool:
    """Ajoute un compte en utilisant uniquement les cookies - Version améliorée"""
    try:
        logger.info("Ajout du compte avec cookies (version améliorée)...")

        # Parse cookies into dictionary
        cookies_dict = parse_cookies_string(TWITTER_COOKIES)

        # Validate cookies format
        is_valid, missing_cookies = validate_cookies_format(cookies_dict)
        if not is_valid:
            logger.error(f"Impossible d'ajouter le compte - cookies invalides: {', '.join(missing_cookies)}")
            return False

        # Generate a unique username based on auth_token
        auth_token = cookies_dict.get('auth_token', '')
        if auth_token:
            # Use first 8 chars of auth_token for uniqueness
            username_suffix = auth_token[:8]
        else:
            # Fallback to cookie hash
            cookie_hash = hashlib.md5(TWITTER_COOKIES.encode()).hexdigest()[:8]
            username_suffix = cookie_hash

        fake_username = f"cookie_user_{username_suffix}"
        fake_email = f"{fake_username}@cookies.local"

        # Check if account already exists
        existing_accounts = await api.pool.accounts_info()
        for acc in existing_accounts:
            acc_username = acc.get('username') if isinstance(acc, dict) else getattr(acc, 'username', '')
            if acc_username == fake_username:
                logger.info(f"Compte existant trouvé: {fake_username}")
                # Try to reactivate if inactive
                try:
                    await api.pool.set_active(fake_username, True)
                    logger.info(f"Compte {fake_username} réactivé")
                except:
                    pass
                return True

        # Add new account with enhanced cookie format
        try:
            await api.pool.add_account(
                username=fake_username,
                password="cookie_based_auth",  # Placeholder password
                email=fake_email,
                email_password="",
                cookies=TWITTER_COOKIES
            )

            logger.info(f"✓ Compte ajouté avec succès: {fake_username}")

            # Wait a moment for the account to be processed
            await asyncio.sleep(1)

            # Verify account was added and try to activate it
            accounts = await api.pool.accounts_info()
            for acc in accounts:
                acc_username = acc.get('username') if isinstance(acc, dict) else getattr(acc, 'username', '')
                if acc_username == fake_username:
                    try:
                        await api.pool.set_active(fake_username, True)
                        logger.info(f"✓ Compte {fake_username} activé")
                    except Exception as activate_error:
                        logger.warning(f"Impossible d'activer le compte: {activate_error}")
                    break

            return True

        except Exception as add_error:
            logger.error(f"Erreur lors de l'ajout du compte: {add_error}")

            # Try alternative method with individual cookie values
            try:
                logger.info("Tentative d'ajout avec méthode alternative...")

                # Create a more structured cookie format
                structured_cookies = []
                for key, value in cookies_dict.items():
                    structured_cookies.append(f"{key}={value}")
                structured_cookie_string = "; ".join(structured_cookies)

                await api.pool.add_account(
                    username=fake_username,
                    password="cookie_auth_alt",
                    email=fake_email,
                    email_password="",
                    cookies=structured_cookie_string
                )

                logger.info(f"✓ Compte ajouté avec méthode alternative: {fake_username}")
                return True

            except Exception as alt_error:
                logger.error(f"Méthode alternative échouée: {alt_error}")
                return False

    except Exception as e:
        logger.error(f"Échec de l'ajout du compte avec cookies: {e}")
        return False


async def ensure_active_account() -> bool:
    """Assure qu'au moins un compte est actif"""
    try:
        accounts = await api.pool.accounts_info()

        # Check for active accounts
        active_accounts = []
        for acc in accounts:
            is_active = acc.get('active') if isinstance(acc, dict) else getattr(acc, 'active', False)
            if is_active:
                active_accounts.append(acc)

        if active_accounts:
            logger.info(f"✓ {len(active_accounts)} compte(s) actif(s) trouvé(s)")
            return True

        # Try to activate existing accounts
        if accounts:
            logger.info("Tentative d'activation des comptes existants...")
            for acc in accounts:
                acc_username = acc.get('username') if isinstance(acc, dict) else getattr(acc, 'username', '')
                try:
                    await api.pool.set_active(acc_username, True)
                    logger.info(f"Compte {acc_username} activé")
                    return True
                except Exception as e:
                    logger.warning(f"Impossible d'activer {acc_username}: {e}")

            # Try login_all as last resort
            try:
                logger.info("Tentative de login général...")
                await api.pool.login_all()
                await asyncio.sleep(2)

                # Re-check for active accounts
                accounts = await api.pool.accounts_info()
                for acc in accounts:
                    is_active = acc.get('active') if isinstance(acc, dict) else getattr(acc, 'active', False)
                    if is_active:
                        logger.info("✓ Au moins un compte activé par login général")
                        return True
            except Exception as login_error:
                logger.warning(f"Login général échoué: {login_error}")

        return False

    except Exception as e:
        logger.error(f"Erreur lors de la vérification des comptes actifs: {e}")
        return False


async def login() -> bool:
    """Login function using only cookies - Enhanced version."""
    global api

    if not validate_credentials():
        return False

    try:
        logger.info("Configuration du compte Twitter avec cookies (version améliorée)...")

        # Check existing accounts first
        accounts = await api.pool.accounts_info()
        logger.info(f"Comptes existants trouvés: {len(accounts)}")

        # If no accounts exist, add one with cookies
        if not accounts:
            logger.info("Aucun compte existant - ajout via cookies...")
            if not await add_account_with_cookies():
                logger.error("Impossible d'ajouter le compte avec cookies")
                return False
        else:
            # Try to ensure at least one account is active
            if not await ensure_active_account():
                logger.warning("Aucun compte actif - tentative d'ajout d'un nouveau compte...")
                if not await add_account_with_cookies():
                    logger.error("Impossible d'ajouter un nouveau compte")
                    return False

        # Final verification
        accounts = await api.pool.accounts_info()
        if not accounts:
            logger.error("Aucun compte disponible après configuration")
            return False

        # Check for at least one active account
        active_count = 0
        for acc in accounts:
            is_active = acc.get('active') if isinstance(acc, dict) else getattr(acc, 'active', False)
            if is_active:
                active_count += 1

        if active_count == 0:
            logger.warning("Aucun compte actif détecté, mais poursuite du processus...")

        logger.info(f"✓ Configuration terminée: {len(accounts)} comptes, {active_count} actifs")
        return True

    except Exception as e:
        logger.error(f"Échec de la connexion: {e}")
        return False


def is_potential_product_buyer(tweet_data: Dict, product_focus: str = "all") -> bool:
    """Filter for tweets from potential buyers of trending Amazon products."""
    try:
        text = tweet_data.get('text', '').lower()

        # Product-specific buyer signals
        product_keywords = {
            "stanley": [
                # Stanley Tumbler specific
                'stanley', 'tumbler', 'water bottle', 'hydration', 'insulated', 'stainless steel',
                'keeps drinks cold', 'keeps drinks hot', 'daily hydration', 'gym bottle',
                'travel mug', 'coffee tumbler', 'ice cold', 'thermal', 'leak proof',
                'need a good water bottle', 'looking for water bottle', 'bottle recommendation',
                'favorite water bottle', 'best water bottle', 'stanley cup', 'quencher',
                'owala', 'hydroflask', 'yeti', 'thermos', 'contigo'
            ],
            "firetv": [
                # Fire TV Stick 4K specific
                'fire tv', 'fire stick', 'streaming device', 'roku', 'chromecast', 'apple tv',
                'netflix', 'prime video', 'hulu', 'disney plus', 'streaming', '4k streaming',
                'smart tv', 'cord cutting', 'cut the cord', 'streaming setup', 'home theater',
                'need streaming device', 'best streaming device', 'tv stick recommendation',
                'amazon fire', 'fire tv stick', 'streaming stick', 'binge watching'
            ],
            "earbuds": [
                # TOZO T6 and wireless earbuds
                'earbuds', 'wireless earbuds', 'bluetooth earbuds', 'tozo', 'airpods',
                'headphones', 'wireless headphones', 'true wireless', 'noise canceling',
                'budget earbuds', 'cheap earbuds', 'affordable earbuds', 'gym earbuds',
                'running earbuds', 'workout headphones', 'music lover', 'audio quality',
                'need new earbuds', 'earbuds recommendation', 'lost my earbuds',
                'broken earbuds', 'best earbuds', 'wireless audio'
            ],
            "skincare": [
                # Mighty Patch and skincare
                'acne', 'pimple', 'pimple patch', 'mighty patch', 'skincare', 'breakout',
                'blemish', 'skin care routine', 'clear skin', 'acne treatment',
                'spot treatment', 'pimple patches work', 'hydrocolloid', 'skin problems',
                'acne patches', 'skincare products', 'beauty routine', 'skin health',
                'need skincare help', 'acne struggle', 'pimple emergency', 'skin care tips'
            ],
            "hydration": [
                # General hydration products (Stanley/Owala overlap)
                'hydration', 'water intake', 'drink more water', 'staying hydrated',
                'water bottle', 'daily water goal', 'hydration reminder', 'water tracker',
                'dehydrated', 'need to drink water', 'water challenge', 'hydrate or die',
                'wellness', 'health goals', 'fitness goals', 'gym essentials',
                'work from home essentials', 'back to school', 'college essentials'
            ]
        }

        # General buyer intent signals
        buyer_signals = [
            # Direct purchase intent
            'need', 'looking for', 'want to buy', 'shopping for', 'in the market for',
            'recommendations', 'recommend', 'suggest', 'advice', 'help me find',
            'where to buy', 'best place to buy', 'good deal', 'discount', 'sale',
            'coupon', 'promo code', 'amazon', 'prime day', 'black friday',
            
            # Problem/pain point expressions
            'broken', 'lost', 'old', 'worn out', 'not working', 'need new',
            'upgrade', 'replace', 'replacement', 'better than', 'alternative to',
            
            # Lifestyle/context signals
            'back to school', 'college', 'dorm', 'office', 'work from home',
            'gym', 'workout', 'fitness', 'travel', 'vacation', 'holiday',
            'gift', 'birthday', 'christmas', 'mother\'s day', 'father\'s day',
            
            # Social proof seeking
            'worth it', 'reviews', 'anyone tried', 'thoughts on', 'experience with',
            'pros and cons', 'honest review', 'regret buying', 'love mine',
            'highly recommend', 'game changer', 'life changing'
        ]

        # Check for product-specific keywords
        has_product_keywords = False
        if product_focus == "all" or product_focus not in product_keywords:
            # Check all product categories
            for category_keywords in product_keywords.values():
                if any(keyword in text for keyword in category_keywords):
                    has_product_keywords = True
                    break
        else:
            # Check specific product category
            has_product_keywords = any(keyword in text for keyword in product_keywords[product_focus])

        # Check for buyer intent signals
        has_buyer_signals = any(signal in text for signal in buyer_signals)

        # Quality filters
        is_long_enough = len(text) > 20
        not_spam = not any(spam_word in text for spam_word in [
            'buy now', 'click here', 'free money', 'get rich', 'mlm', 'pyramid',
            'work from home opportunity', 'make money fast', 'affiliate link'
        ])
        not_too_many_hashtags = text.count('#') <= 5
        not_too_many_mentions = text.count('@') <= 3
        no_excessive_caps = sum(1 for c in text if c.isupper()) / len(text) < 0.4 if text else False

        # High-value buyer indicators (even without specific product keywords)
        high_value_indicators = [
            'just bought', 'purchased', 'ordered', 'delivered', 'unboxing',
            'review', 'testing', 'trying out', 'first impressions',
            'amazon purchase', 'prime delivery', 'two day shipping'
        ]
        
        has_high_value_indicators = any(indicator in text for indicator in high_value_indicators)

        return ((has_product_keywords or has_high_value_indicators) and 
                (has_buyer_signals or has_high_value_indicators) and
                is_long_enough and not_spam and not_too_many_hashtags and 
                not_too_many_mentions and no_excessive_caps)

    except Exception as e:
        logger.warning(f"Error in buyer filter: {e}")
        return False


def extract_tweet_data_bot_format(tweet: Tweet) -> Optional[Dict]:
    """Extract tweet data and return in bot-compatible format."""
    try:
        # Vérifier que le tweet a du contenu
        tweet_text = getattr(tweet, 'rawContent', '') or getattr(tweet, 'text', '')
        if not tweet_text or not tweet_text.strip():
            return None

        # Timestamp
        created_at = datetime.now().isoformat()
        if hasattr(tweet, 'date') and tweet.date:
            created_at = tweet.date.isoformat()

        # Tweet ID et URL
        tweet_id = str(tweet.id) if hasattr(tweet, 'id') and tweet.id else None
        tweet_url = getattr(tweet, 'url', '')

        if not tweet_id:
            # Générer un ID de fallback
            fallback_hash = hashlib.md5(f"{tweet_text}_{created_at}".encode()).hexdigest()[:16]
            tweet_id = fallback_hash
            if not tweet_url:
                tweet_url = f"https://x.com/status/{fallback_hash}"

        # Auteur
        author = "unknown"
        if hasattr(tweet, 'user') and tweet.user:
            if hasattr(tweet.user, 'username') and tweet.user.username:
                author = tweet.user.username
            elif hasattr(tweet.user, 'displayname') and tweet.user.displayname:
                author = tweet.user.displayname

        # Médias
        media = []
        if hasattr(tweet, "media") and tweet.media:
            # If tweet.media is already a list, use it as-is;
            # otherwise wrap the single object in a list.
            if isinstance(tweet.media, list):
                media_items = tweet.media
            else:
                media_items = [tweet.media]

            for media_item in media_items:
                media_url = getattr(media_item, "mediaUrl", None) or getattr(media_item, "url", None)
                if media_url:
                    media.append(media_url)

        return {
            "id": tweet_id,
            "text": tweet_text.strip(),
            "url": tweet_url,
            "created_at": created_at,
            "author": author,
            "media": media
        }

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des données du tweet: {e}")
        return None


async def fetch_tweets(source_type: str, source: str, limit: int = 20, product_focus: str = "all") -> List[Dict]:
    """
    Fonction principale pour récupérer des tweets - PRODUCT BUYER FOCUSED VERSION
    Compatible avec les appels de main.py
    
    Args:
        source_type: "timeline", "user", or "search"
        source: username or search query
        limit: number of tweets to fetch
        product_focus: "stanley", "firetv", "earbuds", "skincare", "hydration", or "all"
    """
    global api

    # Initialiser l'API si nécessaire
    if api is None:
        if not setup_driver():
            logger.error("Impossible d'initialiser l'API twscrape")
            return []

    # Se connecter si nécessaire
    if not await login():
        logger.error("Échec de la connexion à Twitter")
        return []

    try:
        if source_type == "timeline":
            return await async_scrape_product_buyers(limit, product_focus)
        elif source_type == "user":
            # For user requests, still focus on product buyers
            logger.info(f"Requête utilisateur @{source} convertie en recherche d'acheteurs potentiels")
            return await async_scrape_product_buyers(limit, product_focus)
        elif source_type == "search":
            # Use the search query but filter for buyer intent
            return await async_scrape_search_for_buyers(source, limit, product_focus)
        else:
            logger.error(f"Type de source non supporté: {source_type}")
            return []
    except Exception as e:
        logger.error(f"Erreur dans fetch_tweets: {e}")
        return []


async def async_scrape_product_buyers(limit: int = 20, product_focus: str = "all") -> List[Dict]:
    """Scraper asynchrone optimisé pour trouver des acheteurs potentiels de produits tendance."""
    try:
        logger.info(f"Recherche d'acheteurs potentiels (focus: {product_focus}, limite: {limit})")

        # Utiliser la nouvelle méthode directe pour trouver des acheteurs
        tweets = await get_product_buyer_tweets_direct(limit, product_focus)

        if tweets:
            # Sauvegarder dans Excel
            filename = f"product_buyer_tweets_{product_focus}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            await save_tweets_to_excel(tweets, filename)
            logger.info(f"Acheteurs potentiels trouvés: {len(tweets)} tweets")
        else:
            logger.warning("Aucun acheteur potentiel trouvé")

        return tweets

    except Exception as e:
        logger.error(f"Erreur dans async_scrape_product_buyers: {e}")
        return []


async def async_scrape_search_for_buyers(query: str, limit: int = 20, product_focus: str = "all") -> List[Dict]:
    """Scraper asynchrone pour rechercher des acheteurs basé sur une requête."""
    try:
        logger.info(f"Recherche d'acheteurs avec requête: '{query}' (focus: {product_focus})")
        
        # Combine the user query with buyer intent signals
        enhanced_query = f"({query}) (need OR looking for OR recommend OR review OR worth it OR broken OR lost) -filter:replies -is:retweet lang:en"
        
        tweets = await gather(api.search(enhanced_query, limit=limit * 2))  # Get more to filter
        
        processed_tweets = []
        for tweet in tweets:
            tweet_data = extract_tweet_data_bot_format(tweet)
            if tweet_data and is_potential_product_buyer(tweet_data, product_focus):
                processed_tweets.append(tweet_data)
                if len(processed_tweets) >= limit:
                    break
        
        if processed_tweets:
            filename = f"search_buyer_tweets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            await save_tweets_to_excel(processed_tweets, filename)
        
        return processed_tweets

    except Exception as e:
        logger.error(f"Erreur dans async_scrape_search_for_buyers: {e}")
        return []


async def get_product_buyer_tweets_direct(limit: int = 20, product_focus: str = "all") -> List[Dict]:
    """Récupère les tweets d'acheteurs potentiels directement - Focus sur les 5 produits tendance."""
    global api

    try:
        # Product-specific search queries optimized for buyer intent
        product_queries = {
            "stanley": [
                # Stanley Tumbler buyer queries
                "(stanley tumbler OR stanley cup OR stanley quencher) (need OR looking for OR recommend OR review OR worth it OR broken OR lost) min_faves:3 -filter:replies -is:retweet lang:en",
                "(water bottle OR insulated tumbler) (need new OR recommendation OR broken OR best) min_faves:2 -filter:replies -is:retweet lang:en",
                "(hydration OR staying hydrated OR gym bottle) (looking for OR need OR advice) -filter:replies -is:retweet lang:en",
            ],
            "firetv": [
                # Fire TV Stick buyer queries  
                "(fire tv stick OR fire stick OR streaming device) (need OR looking for OR recommend OR review OR worth it) min_faves:3 -filter:replies -is:retweet lang:en",
                "(roku OR chromecast OR apple tv OR streaming stick) (which one OR recommendation OR best) min_faves:2 -filter:replies -is:retweet lang:en",
                "(cord cutting OR streaming setup OR smart tv) (help OR advice OR need) -filter:replies -is:retweet lang:en",
            ],
            "earbuds": [
                # TOZO T6 and wireless earbuds buyer queries
                "(wireless earbuds OR bluetooth earbuds) (need OR looking for OR recommend OR review OR broken) min_faves:3 -filter:replies -is:retweet lang:en",
                "(tozo earbuds OR budget earbuds OR cheap earbuds) (worth it OR review OR recommendation) min_faves:2 -filter:replies -is:retweet lang:en",
                "(airpods alternative OR affordable earbuds) (need OR looking for OR advice) -filter:replies -is:retweet lang:en",
            ],
            "skincare": [
                # Mighty Patch buyer queries
                "(pimple patches OR acne patches OR mighty patch) (work OR worth it OR review OR need) min_faves:3 -filter:replies -is:retweet lang:en",
                "(skincare routine OR acne treatment) (help OR advice OR recommendation OR need) min_faves:2 -filter:replies -is:retweet lang:en",
                "(breakout OR pimple emergency OR acne struggle) (help OR need OR advice) -filter:replies -is:retweet lang:en",
            ],
            "hydration": [
                # General hydration products buyer queries
                "(water bottle OR hydration OR owala OR hydroflask) (need OR recommend OR review OR broken) min_faves:3 -filter:replies -is:retweet lang:en",
                "(drink more water OR staying hydrated OR water intake) (need help OR advice OR reminder) min_faves:2 -filter:replies -is:retweet lang:en",
                "(gym essentials OR fitness goals OR wellness) (water bottle OR hydration) -filter:replies -is:retweet lang:en",
            ]
        }

        # General high-intent buyer queries (catch-all)
        general_buyer_queries = [
            # High buyer intent across all products
            "(amazon OR prime) (just ordered OR delivered OR unboxing OR review) (stanley OR fire tv OR earbuds OR skincare OR water bottle) min_faves:5 -filter:replies -is:retweet lang:en",
            "(need new OR looking for OR recommend OR broken OR lost) (tumbler OR streaming device OR earbuds OR acne patch OR water bottle) min_faves:3 -filter:replies -is:retweet lang:en",
            "(back to school OR college OR dorm OR office essentials) (water bottle OR streaming OR earbuds OR skincare) min_faves:2 -filter:replies -is:retweet lang:en",
            "(gift ideas OR birthday gift OR holiday gift) (stanley OR fire stick OR earbuds OR skincare) min_faves:2 -filter:replies -is:retweet lang:en",
        ]

        # Choose queries based on product focus
        if product_focus == "all":
            queries_to_try = []
            for category_queries in product_queries.values():
                queries_to_try.extend(category_queries[:2])  # Take first 2 from each category
            queries_to_try.extend(general_buyer_queries)
        elif product_focus in product_queries:
            queries_to_try = product_queries[product_focus] + general_buyer_queries[:2]
        else:
            queries_to_try = general_buyer_queries

        # Try each query method
        for i, query in enumerate(queries_to_try[:8]):  # Limit to 8 queries to avoid rate limits
            try:
                logger.info(f"Essai requête acheteur {i+1}: {query[:50]}...")
                tweets = await gather(api.search(query, limit=limit))

                if tweets and len(tweets) > 0:
                    logger.info(f"✓ Requête {i+1} réussie: {len(tweets)} tweets")
                    processed_tweets = []

                    for tweet in tweets:
                        tweet_data = extract_tweet_data_bot_format(tweet)
                        if tweet_data and is_potential_product_buyer(tweet_data, product_focus):
                            processed_tweets.append(tweet_data)

                        if len(processed_tweets) >= limit:
                            break

                    if processed_tweets:
                        return processed_tweets[:limit]

            except Exception as method_error:
                logger.warning(f"Requête {i+1} échouée: {method_error}")
                continue

        # Fallback: Try to get from trending product-related accounts
        product_accounts = {
            "stanley": ["stanley_brand", "hydroflask", "yeti", "owalalife"],
            "firetv": ["amazonfiretv", "amazon", "roku", "googletv"],
            "earbuds": ["tozo_official", "anker", "soundcore", "jabra"],
            "skincare": ["herocosmetics", "theordinary", "cerave", "neutrogena"],
            "hydration": ["stanley_brand", "owalalife", "hydroflask", "yeti"]
        }

        accounts_to_try = []
        if product_focus in product_accounts:
            accounts_to_try = product_accounts[product_focus][:2]
        else:
            # Mix of all product accounts
            for accts in product_accounts.values():
                accounts_to_try.extend(accts[:1])

        for account in accounts_to_try[:4]:  # Limit to avoid rate limits
            try:
                logger.info(f"Essai compte produit: @{account}")
                account_tweets = await gather(api.user_tweets(account, limit=10))
                
                if account_tweets:
                    processed_tweets = []
                    for tweet in account_tweets:
                        tweet_data = extract_tweet_data_bot_format(tweet)
                        if tweet_data:
                            # For brand accounts, look at replies/mentions that might indicate buyer interest
                            processed_tweets.append(tweet_data)
                    
                    if processed_tweets:
                        logger.info(f"✓ Trouvé {len(processed_tweets)} tweets du compte @{account}")
                        return processed_tweets[:limit]
                        
            except Exception as account_error:
                logger.warning(f"Échec compte @{account}: {account_error}")
                continue

        logger.warning("Toutes les méthodes de recherche d'acheteurs ont échoué")
        return []

    except Exception as e:
        logger.error(f"Erreur dans get_product_buyer_tweets_direct: {e}")
        return []


async def fetch_buyer_replies_and_mentions(limit: int = 10) -> List[Dict]:
    """Fetch replies and mentions that might indicate buyer interest."""
    try:
        # Search for tweets with high engagement that mention our target products
        high_engagement_queries = [
            "(stanley OR tumbler OR water bottle) (\"just got\" OR \"love mine\" OR \"highly recommend\" OR \"game changer\") min_faves:10 min_retweets:2 lang:en",
            "(fire tv stick OR streaming) (\"works great\" OR \"love it\" OR \"worth it\" OR \"highly recommend\") min_faves:8 min_retweets:2 lang:en",
            "(wireless earbuds OR tozo) (\"great sound\" OR \"love them\" OR \"worth it\" OR \"highly recommend\") min_faves:8 min_retweets:2 lang:en",
            "(pimple patches OR skincare) (\"actually work\" OR \"love these\" OR \"life saver\" OR \"highly recommend\") min_faves:5 min_retweets:1 lang:en"
        ]

        all_tweets = []
        for query in high_engagement_queries[:3]:  # Limit queries
            try:
                tweets = await gather(api.search(query, limit=limit//3))
                for tweet in tweets:
                    tweet_data = extract_tweet_data_bot_format(tweet)
                    if tweet_data:
                        all_tweets.append(tweet_data)
            except Exception as e:
                logger.warning(f"Failed high engagement query: {e}")
                continue

        return all_tweets[:limit]

    except Exception as e:
        logger.error(f"Error in fetch_buyer_replies_and_mentions: {e}")
        return []


async def save_tweets_to_excel(tweets_data: List[Dict], filename: str):
    """Sauvegarde les tweets dans un fichier Excel avec analyse d'intention d'achat."""
    if not tweets_data:
        return

    try:
        # Convertir au format Excel avec analyse
        excel_data = []
        for tweet in tweets_data:
            text = tweet.get('text', '')
            
            # Analyser l'intention d'achat
            buyer_score = analyze_buyer_intent(text)
            product_category = detect_product_category(text)
            urgency_level = detect_urgency(text)
            
            media_str = ', '.join(tweet.get('media', [])) if tweet.get('media') else "No Images"
            excel_data.append([
                tweet.get('text', ''),
                tweet.get('author', ''),
                tweet.get('created_at', '').split('T')[0],
                tweet.get('url', ''),
                media_str,
                buyer_score,
                product_category,
                urgency_level
            ])

        df = pd.DataFrame(excel_data, columns=[
            "Tweet", "Author", "Date", "Link", "Images", 
            "Buyer Score", "Product Category", "Urgency Level"
        ])
        
        # Trier par score d'acheteur (plus élevé = meilleur prospect)
        df = df.sort_values("Buyer Score", ascending=False)
        
        df.to_excel(filename, index=False)
        logger.info(f"Tweets acheteurs sauvegardés dans {filename}")

    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde Excel: {e}")


def analyze_buyer_intent(text: str) -> int:
    """Analyse l'intention d'achat et retourne un score de 1-10."""
    text = text.lower()
    score = 0
    
    # Signaux d'intention forte (3-4 points chacun)
    high_intent = ['need', 'looking for', 'want to buy', 'shopping for', 'where to buy']
    for signal in high_intent:
        if signal in text:
            score += 4
            break
    
    # Signaux de problème à résoudre (2-3 points)
    problem_signals = ['broken', 'lost', 'not working', 'worn out', 'need new', 'replace']
    for signal in problem_signals:
        if signal in text:
            score += 3
            break
    
    # Recherche de recommandations (2 points)
    recommendation_signals = ['recommend', 'suggestions', 'advice', 'which one', 'best']
    for signal in recommendation_signals:
        if signal in text:
            score += 2
            break
    
    # Signaux de timing/urgence (1-2 points)
    urgency_signals = ['asap', 'urgent', 'today', 'now', 'emergency']
    for signal in urgency_signals:
        if signal in text:
            score += 2
            break
    
    # Signaux contextuels (1 point)
    context_signals = ['back to school', 'college', 'gift', 'birthday', 'holiday']
    for signal in context_signals:
        if signal in text:
            score += 1
            break
    
    return min(score, 10)  # Cap at 10


def detect_product_category(text: str) -> str:
    """Détecte la catégorie de produit mentionnée."""
    text = text.lower()
    
    categories = {
        'Stanley/Hydration': ['stanley', 'tumbler', 'water bottle', 'hydration', 'owala', 'hydroflask'],
        'Fire TV/Streaming': ['fire tv', 'fire stick', 'streaming', 'roku', 'chromecast', 'apple tv'],
        'Earbuds/Audio': ['earbuds', 'headphones', 'tozo', 'airpods', 'wireless', 'bluetooth'],
        'Skincare/Beauty': ['pimple', 'acne', 'skincare', 'mighty patch', 'beauty', 'skin care'],
        'General/Other': []
    }
    
    for category, keywords in categories.items():
        if any(keyword in text for keyword in keywords):
            return category
    
    return 'General/Other'


def detect_urgency(text: str) -> str:
    """Détecte le niveau d'urgence de l'achat."""
    text = text.lower()
    
    if any(word in text for word in ['urgent', 'asap', 'emergency', 'now', 'today', 'immediately']):
        return 'High'
    elif any(word in text for word in ['soon', 'this week', 'need by', 'before']):
        return 'Medium'
    else:
        return 'Low'


# COMPATIBILITÉ: Fonctions synchrones pour la compatibilité avec l'ancien code
def scrape_user_tweets(username: str, limit: int = 20, product_focus: str = "all") -> List[Dict]:
    """Version synchrone du scraping utilisateur - Focus acheteurs potentiels."""
    try:
        if not setup_driver():
            logger.error("Impossible d'initialiser l'API twscrape")
            return []

        return asyncio.run(async_buyer_wrapper(limit, product_focus))
    except Exception as e:
        logger.error(f"Erreur dans scrape_user_tweets: {e}")
        return []


def scrape_search_tweets(query: str, limit: int = 20, product_focus: str = "all") -> List[Dict]:
    """Version synchrone du scraping de recherche - Focus acheteurs potentiels."""
    try:
        if not setup_driver():
            logger.error("Impossible d'initialiser l'API twscrape")
            return []

        return asyncio.run(async_search_buyer_wrapper(query, limit, product_focus))
    except Exception as e:
        logger.error(f"Erreur dans scrape_search_tweets: {e}")
        return []


async def async_buyer_wrapper(limit: int, product_focus: str) -> List[Dict]:
    """Wrapper asynchrone unifié pour le scraping d'acheteurs."""
    if not await login():
        logger.error("Échec de la connexion")
        return []

    return await get_product_buyer_tweets_direct(limit, product_focus)


async def async_search_buyer_wrapper(query: str, limit: int, product_focus: str) -> List[Dict]:
    """Wrapper asynchrone pour la recherche d'acheteurs."""
    if not await login():
        logger.error("Échec de la connexion")
        return []

    return await async_scrape_search_for_buyers(query, limit, product_focus)


# Test functions
async def test_product_buyer_scraper():
    """Test le scraper pour acheteurs de produits."""
    logger.info("=== TESTING PRODUCT BUYER SCRAPER ===")
    
    try:
        # Test 1: Initialize API
        logger.info("Test 1: Initializing API...")
        if not setup_driver():
            logger.error("❌ Failed to initialize API")
            return False
        logger.info("✅ API initialized successfully")

        # Test 2: Validate credentials
        logger.info("Test 2: Validating credentials...")
        if not validate_credentials():
            logger.error("❌ Credentials validation failed")
            return False
        logger.info("✅ Credentials validated")

        # Test 3: Login
        logger.info("Test 3: Testing login...")
        if not await login():
            logger.error("❌ Login failed")
            return False
        logger.info("✅ Login successful")

        # Test 4: Test each product category
        product_categories = ["stanley", "firetv", "earbuds", "skincare", "all"]
        
        for category in product_categories[:3]:  # Test first 3 to avoid rate limits
            logger.info(f"Test 4.{category}: Fetching {category} buyer tweets...")
            tweets = await get_product_buyer_tweets_direct(3, category)
            
            if tweets:
                logger.info(f"✅ {category}: Found {len(tweets)} potential buyer tweets")
                for i, tweet in enumerate(tweets, 1):
                    buyer_score = analyze_buyer_intent(tweet['text'])
                    product_cat = detect_product_category(tweet['text'])
                    logger.info(f"  Tweet {i} (Score: {buyer_score}, Category: {product_cat}): {tweet['text'][:80]}...")
            else:
                logger.warning(f"⚠️ {category}: No buyer tweets found")

        # Test 5: Test main interface
        logger.info("Test 5: Testing main fetch_tweets interface...")
        tweets = await fetch_tweets("timeline", "", 3, "stanley")
        if tweets:
            logger.info(f"✅ Main interface working: {len(tweets)} tweets")
        else:
            logger.warning("⚠️ Main interface returned no tweets")

        logger.info("=== PRODUCT BUYER TESTS COMPLETED ===")
        return True

    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        return False


# Fonction utilitaire pour analyser les résultats
def analyze_scraped_buyers(tweets: List[Dict]) -> Dict:
    """Analyse les tweets récupérés pour fournir des insights sur les acheteurs."""
    if not tweets:
        return {"error": "No tweets to analyze"}
    
    analysis = {
        "total_tweets": len(tweets),
        "high_intent_buyers": 0,
        "medium_intent_buyers": 0,
        "low_intent_buyers": 0,
        "product_categories": {},
        "urgency_levels": {"High": 0, "Medium": 0, "Low": 0},
        "top_buyer_signals": [],
        "average_buyer_score": 0
    }
    
    total_score = 0
    buyer_signals = {}
    
    for tweet in tweets:
        text = tweet.get('text', '')
        score = analyze_buyer_intent(text)
        category = detect_product_category(text)
        urgency = detect_urgency(text)
        
        total_score += score
        
        # Categorize by buyer intent
        if score >= 7:
            analysis["high_intent_buyers"] += 1
        elif score >= 4:
            analysis["medium_intent_buyers"] += 1
        else:
            analysis["low_intent_buyers"] += 1
        
        # Track product categories
        if category in analysis["product_categories"]:
            analysis["product_categories"][category] += 1
        else:
            analysis["product_categories"][category] = 1
        
        # Track urgency levels
        analysis["urgency_levels"][urgency] += 1
        
        # Track buyer signals
        signals = ['need', 'looking for', 'recommend', 'broken', 'lost', 'worth it']
        for signal in signals:
            if signal in text.lower():
                if signal in buyer_signals:
                    buyer_signals[signal] += 1
                else:
                    buyer_signals[signal] = 1
    
    analysis["average_buyer_score"] = round(total_score / len(tweets), 2)
    analysis["top_buyer_signals"] = sorted(buyer_signals.items(), key=lambda x: x[1], reverse=True)[:5]
    
    return analysis


if __name__ == "__main__":
    """Run standalone tests"""
    print("Running product buyer scraper tests...")
    
    async def run_tests():
        success = await test_product_buyer_scraper()
        if success:
            print("✅ All tests passed!")
            
            # Demo run
            print("\n=== DEMO RUN ===")
            print("Fetching Stanley tumbler buyer tweets...")
            demo_tweets = await get_product_buyer_tweets_direct(5, "stanley")
            
            if demo_tweets:
                print(f"Found {len(demo_tweets)} potential Stanley buyers:")
                analysis = analyze_scraped_buyers(demo_tweets)
                print(f"Analysis: {analysis}")
            else:
                print("No demo tweets found")
        else:
            print("❌ Some tests failed!")
        return success

    # Run the tests
    asyncio.run(run_tests())
