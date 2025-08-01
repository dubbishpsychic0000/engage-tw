import asyncio
import json
import os
import re
import random
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
import hashlib

from config import logger, MAX_TWEET_LENGTH
from ai_generator import generate_ai_content
from poster import post_content
from twscrape_client import fetch_tweets

class AffiliateProduct:
    """Classe pour représenter un produit d'affiliation"""
    
    def __init__(self, name: str, description: str, affiliate_link: str, 
                 category: str, keywords: List[str], price_range: str = ""):
        self.name = name
        self.description = description
        self.affiliate_link = affiliate_link
        self.category = category
        self.keywords = [kw.lower() for kw in keywords]
        self.price_range = price_range
        self.success_count = 0
        self.view_count = 0
    
    def __dict__(self):
        return {
            'name': self.name,
            'description': self.description,
            'affiliate_link': self.affiliate_link,
            'category': self.category,
            'keywords': self.keywords,
            'price_range': self.price_range,
            'success_count': self.success_count,
            'view_count': self.view_count
        }

class AffiliateProductManager:
    """Gestionnaire des produits d'affiliation"""
    
    def __init__(self, products_file: str = "affiliate_products.json"):
        self.products_file = products_file
        self.products: List[AffiliateProduct] = []
        self.load_products()
    
    def load_products(self):
        """Charge les produits depuis le fichier JSON"""
        try:
            if os.path.exists(self.products_file):
                with open(self.products_file, 'r', encoding='utf-8') as f:
                    products_data = json.load(f)
                    
                self.products = []
                for product_data in products_data:
                    product = AffiliateProduct(
                        name=product_data['name'],
                        description=product_data['description'],
                        affiliate_link=product_data['affiliate_link'],
                        category=product_data['category'],
                        keywords=product_data['keywords'],
                        price_range=product_data.get('price_range', '')
                    )
                    product.success_count = product_data.get('success_count', 0)
                    product.view_count = product_data.get('view_count', 0)
                    self.products.append(product)
                    
                logger.info(f"Chargé {len(self.products)} produits d'affiliation")
            else:
                self.create_default_products()
                
        except Exception as e:
            logger.error(f"Erreur lors du chargement des produits: {e}")
            self.create_default_products()
    
    def save_products(self):
        """Sauvegarde les produits dans le fichier JSON"""
        try:
            products_data = []
            for product in self.products:
                products_data.append({
                    'name': product.name,
                    'description': product.description,
                    'affiliate_link': product.affiliate_link,
                    'category': product.category,
                    'keywords': product.keywords,
                    'price_range': product.price_range,
                    'success_count': product.success_count,
                    'view_count': product.view_count
                })
            
            with open(self.products_file, 'w', encoding='utf-8') as f:
                json.dump(products_data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des produits: {e}")
    
    def create_default_products(self):
        """Crée une liste de produits par défaut"""
        default_products = [
            {
                'name': 'Cours Python Complet',
                'description': 'Formation complète en Python pour débutants et intermédiaires',
                'affiliate_link': 'https://example.com/python-course?ref=bot',
                'category': 'programming',
                'keywords': ['python', 'programming', 'coding', 'learn python', 'development', 'developer'],
                'price_range': '50-100€'
            },
            {
                'name': 'Pack Design Graphique',
                'description': 'Outils et templates pour créer des designs professionnels',
                'affiliate_link': 'https://example.com/design-pack?ref=bot',
                'category': 'design',
                'keywords': ['design', 'graphic design', 'photoshop', 'illustrator', 'creative', 'logo'],
                'price_range': '30-80€'
            },
            {
                'name': 'Guide Marketing Digital',
                'description': 'Stratégies complètes pour réussir en marketing digital',
                'affiliate_link': 'https://example.com/marketing-guide?ref=bot',
                'category': 'marketing',
                'keywords': ['marketing', 'digital marketing', 'social media', 'advertising', 'business'],
                'price_range': '40-120€'
            },
            {
                'name': 'Ebook Productivité',
                'description': 'Méthodes pour optimiser votre productivité et gérer votre temps',
                'affiliate_link': 'https://example.com/productivity-ebook?ref=bot',
                'category': 'productivity',
                'keywords': ['productivity', 'time management', 'efficiency', 'organization', 'workflow'],
                'price_range': '15-30€'
            },
            {
                'name': 'Formation IA & Machine Learning',
                'description': 'Apprenez les bases de l\'IA et du Machine Learning avec des projets pratiques',
                'affiliate_link': 'https://example.com/ai-course?ref=bot',
                'category': 'ai',
                'keywords': ['ai', 'artificial intelligence', 'machine learning', 'deep learning', 'data science'],
                'price_range': '80-200€'
            }
        ]
        
        self.products = []
        for product_data in default_products:
            product = AffiliateProduct(**product_data)
            self.products.append(product)
        
        self.save_products()
        logger.info(f"Créé {len(self.products)} produits par défaut")
    
    def find_matching_products(self, tweet_text: str, max_products: int = 3) -> List[AffiliateProduct]:
        """Trouve les produits correspondant au contenu du tweet"""
        tweet_text_lower = tweet_text.lower()
        
        # Score chaque produit
        product_scores = []
        
        for product in self.products:
            score = 0
            matched_keywords = []
            
            # Vérifier les mots-clés
            for keyword in product.keywords:
                if keyword in tweet_text_lower:
                    score += 10
                    matched_keywords.append(keyword)
                    
                # Vérifier les variations et mots similaires
                variations = self._get_keyword_variations(keyword)
                for variation in variations:
                    if variation in tweet_text_lower and variation not in matched_keywords:
                        score += 5
                        matched_keywords.append(variation)
            
            # Bonus pour correspondance exacte du nom du produit
            if product.name.lower() in tweet_text_lower:
                score += 20
            
            # Bonus pour correspondance de catégorie
            if product.category.lower() in tweet_text_lower:
                score += 8
            
            # Pénalité pour sur-utilisation (éviter le spam)
            if product.success_count > 10:
                score -= 2
            
            if score > 0:
                product_scores.append((product, score, matched_keywords))
        
        # Trier par score et retourner les meilleurs
        product_scores.sort(key=lambda x: x[1], reverse=True)
        
        return [item[0] for item in product_scores[:max_products]]
    
    def _get_keyword_variations(self, keyword: str) -> List[str]:
        """Génère des variations d'un mot-clé"""
        variations = []
        
        # Variations communes
        if keyword == 'python':
            variations = ['py', 'python3', 'python programming']
        elif keyword == 'design':
            variations = ['designer', 'designing', 'designs']
        elif keyword == 'marketing':
            variations = ['marketer', 'advertisement', 'promotion']
        elif keyword == 'productivity':
            variations = ['productive', 'efficient', 'optimize']
        elif keyword == 'ai':
            variations = ['artificial intelligence', 'machine learning', 'ml']
        
        return variations
    
    def update_product_stats(self, product: AffiliateProduct, success: bool = False):
        """Met à jour les statistiques du produit"""
        product.view_count += 1
        if success:
            product.success_count += 1
        self.save_products()

class BuyerIntentDetector:
    """Détecteur d'intention d'achat dans les tweets"""
    
    def __init__(self):
        # Mots-clés indiquant une intention d'achat
        self.buying_keywords = [
            # Intentions directes
            'need', 'want', 'looking for', 'search for', 'trying to find',
            'recommend', 'recommendation', 'suggestions', 'advice',
            'help me', 'which one', 'best way', 'how to',
            
            # Expressions d'achat
            'buy', 'purchase', 'shopping', 'budget', 'price', 'cost',
            'worth it', 'should i get', 'thinking about buying',
            
            # Expressions de besoin
            'struggling with', 'having trouble', 'can\'t figure out',
            'need help', 'any ideas', 'where can i',
            
            # Questions
            'anyone know', 'does anyone', 'has anyone tried',
            'what do you use', 'what would you recommend'
        ]
        
        # Mots-clés de catégories de produits
        self.product_categories = {
            'programming': ['code', 'coding', 'programming', 'developer', 'software', 'app', 'website'],
            'design': ['design', 'logo', 'graphics', 'creative', 'photoshop', 'illustrator'],
            'marketing': ['marketing', 'business', 'social media', 'advertising', 'promotion'],
            'productivity': ['productivity', 'organize', 'time management', 'efficiency', 'workflow'],
            'ai': ['ai', 'artificial intelligence', 'machine learning', 'automation']
        }
        
        # Indicateurs de qualité du prospect
        self.quality_indicators = [
            'budget', 'professional', 'business', 'serious about',
            'investment', 'quality', 'premium', 'enterprise'
        ]
    
    def analyze_tweet(self, tweet_data: Dict) -> Dict:
        """Analyse un tweet pour détecter l'intention d'achat"""
        text = tweet_data.get('text', '').lower()
        
        analysis = {
            'is_potential_buyer': False,
            'intent_score': 0,
            'detected_categories': [],
            'buying_signals': [],
            'quality_score': 0,
            'confidence': 0.0
        }
        
        # Vérifier les mots-clés d'intention d'achat
        for keyword in self.buying_keywords:
            if keyword in text:
                analysis['intent_score'] += 10
                analysis['buying_signals'].append(keyword)
        
        # Vérifier les catégories de produits
        for category, keywords in self.product_categories.items():
            for keyword in keywords:
                if keyword in text:
                    analysis['detected_categories'].append(category)
                    analysis['intent_score'] += 5
                    break
        
        # Vérifier les indicateurs de qualité
        for indicator in self.quality_indicators:
            if indicator in text:
                analysis['quality_score'] += 5
        
        # Calculer la confiance
        total_possible_score = len(self.buying_keywords) * 10 + len(self.product_categories) * 5
        analysis['confidence'] = min(analysis['intent_score'] / total_possible_score, 1.0)
        
        # Déterminer si c'est un acheteur potentiel
        analysis['is_potential_buyer'] = (
            analysis['intent_score'] >= 15 and 
            len(analysis['detected_categories']) > 0
        )
        
        return analysis

class AffiliateMarketingBot:
    """Bot principal pour l'affiliate marketing"""
    
    def __init__(self):
        self.product_manager = AffiliateProductManager()
        self.buyer_detector = BuyerIntentDetector()
        self.processed_tweets = set()
        self.daily_affiliate_count = 0
        self.max_daily_affiliates = 10  # Limite quotidienne
        
        # Charger les tweets déjà traités
        self.load_processed_tweets()
    
    def load_processed_tweets(self):
        """Charge la liste des tweets déjà traités"""
        try:
            if os.path.exists('processed_affiliate_tweets.json'):
                with open('processed_affiliate_tweets.json', 'r') as f:
                    data = json.load(f)
                    self.processed_tweets = set(data.get('tweets', []))
                    self.daily_affiliate_count = data.get('daily_count', 0)
                    
                    # Reset quotidien
                    last_reset = data.get('last_reset', '')
                    today = datetime.now().date().isoformat()
                    if last_reset != today:
                        self.daily_affiliate_count = 0
                        
        except Exception as e:
            logger.error(f"Erreur lors du chargement des tweets traités: {e}")
    
    def save_processed_tweets(self):
        """Sauvegarde la liste des tweets traités"""
        try:
            data = {
                'tweets': list(self.processed_tweets),
                'daily_count': self.daily_affiliate_count,
                'last_reset': datetime.now().date().isoformat()
            }
            with open('processed_affiliate_tweets.json', 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde des tweets traités: {e}")
    
    async def generate_affiliate_reply(self, tweet_text: str, products: List[AffiliateProduct], 
                                     author: str) -> Optional[str]:
        """Génère une réponse d'affiliation personnalisée"""
        try:
            # Choisir le meilleur produit
            primary_product = products[0] if products else None
            if not primary_product:
                return None
            
            # Créer le prompt pour l'IA
            prompt = f"""Write a helpful, natural reply to this tweet as someone knowledgeable about {primary_product.category}.

The person tweeted: "{tweet_text}"

You want to genuinely help them by recommending: {primary_product.name} - {primary_product.description}

Your reply should:
- Be genuinely helpful and not salesy
- Sound natural and conversational  
- Acknowledge their specific need/question
- Briefly mention how the product could help
- Include the link naturally
- Be under {MAX_TWEET_LENGTH-50} characters to leave room for the link
- Use a friendly, knowledgeable tone

Product link: {primary_product.affiliate_link}

Write a helpful reply that includes the link naturally:"""
            
            # Générer la réponse avec l'IA
            reply_content = await generate_ai_content("reply", tweet_text, context=prompt)
            
            if reply_content:
                # Ajouter le lien si pas déjà présent
                if primary_product.affiliate_link not in reply_content:
                    # Calculer l'espace disponible
                    link_space = len(primary_product.affiliate_link) + 1  # +1 pour l'espace
                    max_text_length = MAX_TWEET_LENGTH - link_space
                    
                    if len(reply_content) > max_text_length:
                        reply_content = reply_content[:max_text_length-3] + "..."
                    
                    reply_content += f" {primary_product.affiliate_link}"
                
                # Mettre à jour les statistiques
                self.product_manager.update_product_stats(primary_product)
                
                return reply_content
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération de réponse d'affiliation: {e}")
        
        return None
    
    async def scan_for_buyers(self, limit: int = 50) -> List[Dict]:
        """Scanne les tweets pour trouver des acheteurs potentiels"""
        try:
            logger.info(f"Recherche d'acheteurs potentiels (limite: {limit})")
            
            # Récupérer les tweets
            tweets = await fetch_tweets("timeline", "", limit)
            
            if not tweets:
                logger.warning("Aucun tweet récupéré pour l'analyse d'affiliation")
                return []
            
            potential_buyers = []
            
            for tweet in tweets:
                tweet_id = tweet.get('id', '')
                
                # Ignorer les tweets déjà traités
                if tweet_id in self.processed_tweets:
                    continue
                
                # Analyser l'intention d'achat
                analysis = self.buyer_detector.analyze_tweet(tweet)
                
                if analysis['is_potential_buyer'] and analysis['confidence'] > 0.3:
                    # Trouver les produits correspondants
                    matching_products = self.product_manager.find_matching_products(
                        tweet.get('text', ''), max_products=2
                    )
                    
                    if matching_products:
                        buyer_data = {
                            'tweet': tweet,
                            'analysis': analysis,
                            'products': matching_products,
                            'priority_score': analysis['intent_score'] + analysis['quality_score']
                        }
                        potential_buyers.append(buyer_data)
                        
                        logger.info(f"Acheteur potentiel trouvé: @{tweet.get('author', 'unknown')} "
                                  f"(score: {buyer_data['priority_score']}, "
                                  f"produits: {len(matching_products)})")
            
            # Trier par score de priorité
            potential_buyers.sort(key=lambda x: x['priority_score'], reverse=True)
            
            logger.info(f"Trouvé {len(potential_buyers)} acheteurs potentiels")
            return potential_buyers
            
        except Exception as e:
            logger.error(f"Erreur lors de la recherche d'acheteurs: {e}")
            return []
    
    async def process_affiliate_opportunities(self) -> bool:
        """Traite les opportunités d'affiliation"""
        try:
            # Vérifier la limite quotidienne
            if self.daily_affiliate_count >= self.max_daily_affiliates:
                logger.info(f"Limite quotidienne d'affiliation atteinte ({self.daily_affiliate_count}/{self.max_daily_affiliates})")
                return False
            
            # Rechercher des acheteurs potentiels
            buyers = await self.scan_for_buyers(limit=30)
            
            if not buyers:
                logger.info("Aucun acheteur potentiel trouvé")
                return False
            
            # Traiter les meilleurs prospects
            successful_replies = 0
            
            for buyer_data in buyers[:5]:  # Limiter à 5 prospects par session
                if self.daily_affiliate_count >= self.max_daily_affiliates:
                    break
                
                tweet = buyer_data['tweet']
                products = buyer_data['products']
                
                try:
                    # Générer et poster la réponse
                    reply_content = await self.generate_affiliate_reply(
                        tweet.get('text', ''),
                        products,
                        tweet.get('author', '')
                    )
                    
                    if reply_content:
                        # Délai pour éviter le spam
                        await asyncio.sleep(random.uniform(60, 180))  # 1-3 minutes
                        
                        # Poster la réponse
                        reply_id = await post_content(
                            "reply", 
                            reply_content, 
                            reply_to_id=tweet.get('id')
                        )
                        
                        if reply_id:
                            successful_replies += 1
                            self.daily_affiliate_count += 1
                            self.processed_tweets.add(tweet.get('id', ''))
                            
                            # Marquer comme succès
                            self.product_manager.update_product_stats(products[0], success=True)
                            
                            logger.info(f"✅ Réponse d'affiliation postée: {reply_id} "
                                      f"(produit: {products[0].name})")
                        else:
                            logger.warning("❌ Échec de la publication de la réponse d'affiliation")
                    
                except Exception as reply_error:
                    logger.error(f"Erreur lors du traitement du prospect: {reply_error}")
                    continue
            
            # Sauvegarder l'état
            self.save_processed_tweets()
            
            logger.info(f"Session d'affiliation terminée: {successful_replies} réponses postées")
            return successful_replies > 0
            
        except Exception as e:
            logger.error(f"Erreur lors du traitement des opportunités d'affiliation: {e}")
            return False
    
    def get_statistics(self) -> Dict:
        """Retourne les statistiques du bot d'affiliation"""
        total_products = len(self.product_manager.products)
        total_views = sum(p.view_count for p in self.product_manager.products)
        total_successes = sum(p.success_count for p in self.product_manager.products)
        
        return {
            'total_products': total_products,
            'daily_affiliate_count': self.daily_affiliate_count,
            'max_daily_affiliates': self.max_daily_affiliates,
            'total_views': total_views,
            'total_successes': total_successes,
            'conversion_rate': (total_successes / total_views * 100) if total_views > 0 else 0,
            'processed_tweets': len(self.processed_tweets)
        }

# Fonction principale pour l'intégration avec main.py
async def run_affiliate_marketing() -> bool:
    """Fonction principale pour exécuter le marketing d'affiliation"""
    try:
        bot = AffiliateMarketingBot()
        
        # Afficher les statistiques
        stats = bot.get_statistics()
        logger.info(f"📊 Stats affiliation: {stats['daily_affiliate_count']}/{stats['max_daily_affiliates']} "
                   f"réponses aujourd'hui, {stats['total_products']} produits, "
                   f"{stats['conversion_rate']:.1f}% conversion")
        
        # Exécuter le processus
        return await bot.process_affiliate_opportunities()
        
    except Exception as e:
        logger.error(f"Erreur dans run_affiliate_marketing: {e}")
        return False

if __name__ == "__main__":
    """Test du module d'affiliation"""
    async def test_affiliate_module():
        logger.info("=== TEST DU MODULE D'AFFILIATION ===")
        
        # Test du gestionnaire de produits
        pm = AffiliateProductManager()
        logger.info(f"Produits chargés: {len(pm.products)}")
        
        # Test du détecteur d'intention
        detector = BuyerIntentDetector()
        test_tweets = [
            "I need help learning Python programming, any recommendations?",
            "Looking for good design tools for my startup",
            "Anyone know good marketing strategies for small business?",
            "Just had lunch, nice weather today"
        ]
        
        for tweet in test_tweets:
            analysis = detector.analyze_tweet({'text': tweet})
            logger.info(f"Tweet: {tweet[:50]}...")
            logger.info(f"Acheteur potentiel: {analysis['is_potential_buyer']}, "
                       f"Score: {analysis['intent_score']}, "
                       f"Confiance: {analysis['confidence']:.2f}")
        
        # Test du bot complet
        bot = AffiliateMarketingBot()
        stats = bot.get_statistics()
        logger.info(f"Statistiques: {stats}")
        
        logger.info("=== FIN DU TEST ===")
    
    asyncio.run(test_affiliate_module())
