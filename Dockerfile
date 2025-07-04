# Use official Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port for Flask (if used)
EXPOSE 5000

# Set environment variable so .env is respected
ENV PYTHONUNBUFFERED=1

# Run bot
CMD ["python", "bot.py"]
