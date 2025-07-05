# ✅ Use official Python base image (Linux-based)
FROM python:3.10-slim

# ✅ Set working directory inside the container
WORKDIR /app

# ✅ Copy your project files to the container
COPY . .

# ✅ Upgrade pip (optional but recommended)
RUN pip install --upgrade pip

# ✅ Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# ✅ Expose Flask port (only if using Flask app)
EXPOSE 5000

# ✅ Ensure .env + logs print instantly
ENV PYTHONUNBUFFERED=1

# ✅ Start the bot (replace with main script if not bot.py)
CMD ["python", "bot.py"]
