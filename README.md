# RWA-Analytics
**Hybrid platform for intelligent analysis and monitoring of tokenized real assets (Real World Assets)**

[![Python](https://img.shields.io/badge/Python-3.14.2%2B-blue.svg)](https://www.python.org/)
[![Django](https://img.shields.io/badge/Django-5.1.5-green.svg)](https://www.djangoproject.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**RWA Analytics** is a project that represents a modern hybrid system for analyzing the real tokenized assets (RWA) market.

The project combines two user-friendly interfaces:
**Web application** a powerful analytical dashboard with graphs, comparisons, and in-depth analysis.
**Telegram bot** is fast and mobile assistant with AI-agent for promt requests and notifications.

The system helps users track yield, assess risks, and make informed decisions when investing in RWAs(tokenized treasury bonds, real estate, private credit, and other real assets).

### Key Features

- Monitoring the yield and TVL of popular RWA protocols (BlackRock BUIDL, Ondo OUSG/USDY, Centrifuge, etc.)
- Comparison of assets by key metrics
- Intelligent AI agent that responds in natural language and explains risks
- Alerts and daily notifications in Telegram
- Interactive charts of yield, TVL, and capital inflow dynamics
- Assessing key risks: regulatory, liquidity, oracle failure, redemption delay
- Backtesting and historical analysis (2024–2026)

# Tech stack

### Back
- **Python 3.14+**
- **Django** — main web framework
- **PostgreSQL** — Database

### Telegram Bot 
- **aiogram 3.x** - async framework

### AI and Analytics
- **LangChain / LangGraph** - framework for creating an AI agent
- **LLM**: Gemini 3 Flash / Claude 3.5 Sonnet / Groq (via API)
- **pandas, numpy, scikit-learn, XGBoost** - data analysis
- **Plotly** - interactive visualization

