from openai import OpenAI
import os

# Load environment variables from talentdb/.env
try:
    from dotenv import load_dotenv
    load_dotenv('/Users/avirammizrahi/Desktop/amis/talentdb/.env')
except ImportError:
    pass

# Check if API key is loaded
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("❌ OPENAI_API_KEY not found in environment")
    exit(1)

print(f"✅ API Key loaded (starts with: {api_key[:10]}...)")

# Initialize the client
client = OpenAI(api_key=api_key)

# Test GPT-4o in a chat
try:
    response = client.chat.completions.create(
        model="gpt-4o",  # 👈 updated model
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Explain quantum computing in simple terms."}
        ]
    )

    print("✅ GPT-4o is working!")
    print("Response:", response.choices[0].message.content)
    print("\nModel used:", response.model)
    print("Usage:", response.usage)

except Exception as e:
    print("❌ Error with GPT-4o:", str(e))
    print("Trying fallback to gpt-4o-mini...")

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Explain quantum computing in simple terms."}
            ]
        )

        print("✅ Fallback to gpt-4o-mini works!")
        print("Response:", response.choices[0].message.content)

    except Exception as e2:
        print("❌ Both models failed:", str(e2))
