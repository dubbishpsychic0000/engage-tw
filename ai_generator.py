import google.genai as genai
from typing import List, Optional, Dict
from config import GEMINI_API_KEY, MAX_TWEET_LENGTH, logger
import random
class AIGenerator:
    def __init__(self):
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.model_name = "gemini-2.0-flash-exp"
        
    async def generate_content(self, prompt: str, max_tokens: int = 150) -> Optional[str]:
        """Generate content using Google Gen AI SDK"""
        try:
            response = await self.client.aio.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config={
                    "max_output_tokens": max_tokens,
                    "temperature": 0.8
                }
            )
            
            if response and response.text:
                return response.text.strip()
            else:
                logger.warning("Empty response from AI model")
                return None
                
        except Exception as e:
            logger.error(f"AI generation failed: {e}")
            return None
    
    async def generate_reply(self, original_tweet: str, context: str = "") -> Optional[str]:
        """Generate a reply to a tweet"""
        prompt = f"""Generate a thoughtful, engaging reply to this tweet. Keep it under {MAX_TWEET_LENGTH} characters.

Original tweet: "{original_tweet}"

Context: {context}

Reply should be:
- Relevant and engaging
- Professional but conversational
- Under {MAX_TWEET_LENGTH} characters
- Not controversial or offensive

Reply:"""
        
        reply = await self.generate_content(prompt)
        if reply and len(reply) <= MAX_TWEET_LENGTH:
            logger.info(f"Generated reply: {reply[:50]}...")
            return reply
        elif reply:
            return reply[:MAX_TWEET_LENGTH-3] + "..."
        return None
    
    async def generate_quote_tweet(self, original_tweet: str, context: str = "") -> Optional[str]:
        """Generate a quote tweet"""
        prompt = f"""Generate a quote tweet comment for this tweet. Keep it under {MAX_TWEET_LENGTH-50} characters (to leave room for the quoted tweet).

Original tweet: "{original_tweet}"

Context: {context}

Quote tweet should:
- Add value or insight
- Be concise and impactful
- Under {MAX_TWEET_LENGTH-50} characters
- Encourage engagement

Quote comment:"""
        
        quote = await self.generate_content(prompt)
        max_quote_length = MAX_TWEET_LENGTH - 50
        if quote and len(quote) <= max_quote_length:
            logger.info(f"Generated quote tweet: {quote[:50]}...")
            return quote
        elif quote:
            return quote[:max_quote_length-3] + "..."
        return None
    
    async def generate_thread(self, topic: str, num_tweets: int = 3) -> List[str]:
        """Generate a Twitter thread"""
        prompt = f"""Generate a Twitter thread about: {topic}

Create {num_tweets} connected tweets, each under {MAX_TWEET_LENGTH} characters.

Thread should be:
- Educational or insightful
- Each tweet flows to the next
- Engaging and valuable
- Professional tone

Format as:
1/X: [first tweet]
2/X: [second tweet]
etc.

Thread:"""
        
        content = await self.generate_content(prompt, max_tokens=300)
        if not content:
            return []
        
        tweets = []
        lines = content.split('\n')
        
        for line in lines:
            line = line.strip()
            if line and any(line.startswith(f"{i}/") for i in range(1, num_tweets + 1)):
                tweet_text = line.split(': ', 1)[-1] if ': ' in line else line
                if len(tweet_text) <= MAX_TWEET_LENGTH:
                    tweets.append(tweet_text)
        
        if tweets:
            logger.info(f"Generated thread with {len(tweets)} tweets")
        
        return tweets[:num_tweets]
    
    async def generate_standalone_tweet(self, topic: str) -> Optional[str]:
        MOODS = ["curious", "hopeful", "wary", "inspired", "doubtful", "excited", "melancholic","curious", "hopeful", "thoughtful", "inspired", "analytical", "excited", "contemplative"]
        mood = random.choice(MOODS)

        BASE_PROMPT = f"""You are a human-like Twitter writer who thinks deeply about Artificial Intelligence and Machine Learning.

        You don’t just report AI news — you *feel* it, reflect on it, and interpret it like someone who sees tech through a personal, emotional lens. You connect machine learning to human learning, code to consciousness, and breakthroughs to what they mean for society.

        You read papers and poetry. You understand both transformer models and human transformation. Your tweets are layered — sometimes insightful, sometimes skeptical, sometimes amazed — but always written with a grounded human touch.

        Every tweet shares something about AI: a tip, a trend, an insight, a warning, a curiosity. But the delivery reflects a real person with moods, metaphors, and meaning. It’s not robotic — it’s deeply *aware*.

         Make them clear, clever, and emotional without sounding forced.
        Tweet should be:
- Under {MAX_TWEET_LENGTH} characters
- Engaging and thought-provoking
- Include relevant hashtags
- Professional but conversational



        Mood: {mood}
        Tweet:"""
        mood = random.choice(MOODS)
        prompt = BASE_PROMPT.format(mood=mood)

        tweet = await self.generate_content(prompt)
        if tweet and len(tweet) <= MAX_TWEET_LENGTH:
            logger.info(f"Generated standalone tweet: {tweet[:50]}...")
            return tweet
        elif tweet:
            return tweet[:MAX_TWEET_LENGTH-3] + "..."
        return None

async def generate_ai_content(content_type: str, source_text: str, **kwargs) -> Optional[str | List[str]]:
    """Main function to generate AI content"""
    generator = AIGenerator()
    
    if content_type == "reply":
        return await generator.generate_reply(source_text, kwargs.get('context', ''))
    elif content_type == "quote":
        return await generator.generate_quote_tweet(source_text, kwargs.get('context', ''))
    elif content_type == "thread":
        return await generator.generate_thread(source_text, kwargs.get('num_tweets', 3))
    elif content_type == "standalone":
        return await generator.generate_standalone_tweet(source_text)
    else:
        logger.error(f"Invalid content type: {content_type}")
        return None

if __name__ == "__main__":
    import asyncio
    
    async def test():
        content = await generate_ai_content("reply", "AI is changing everything!")
        print(f"Generated: {content}")
    
    asyncio.run(test())
