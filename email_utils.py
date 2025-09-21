"""
Email utilities for the application.
This module handles all email related functionality using Mailjet API.
"""

import os
import string
import secrets
from datetime import datetime
from mailjet_rest import Client
from dotenv import load_dotenv

# Ensure environment variables are loaded
load_dotenv()

# Email configuration
EMAIL_CONFIG = {
    'API_KEY': os.environ.get('MAILJET_API_KEY'),
    'API_SECRET': os.environ.get('MAILJET_API_SECRET'),
    'FROM_EMAIL': os.environ.get('FROM_EMAIL', 'noreply@examportal.com'),
    'FROM_NAME': 'ExamPortal System',
    'RESET_PASSWORD_URL': os.environ.get('RESET_PASSWORD_URL')
}

def generate_username(full_name, existing_usernames):
    """
    Generate a unique username based on full name.
    
    Args:
        full_name (str): User's full name
        existing_usernames (list): List of existing usernames to avoid duplicates
        
    Returns:
        str: A unique username
    """
    name_parts = full_name.lower().replace(' ', '').replace('.', '')
    base_username = name_parts[:8]

    username = base_username
    counter = 1
    while username in existing_usernames:
        username = f"{base_username}{counter}"
        counter += 1

    return username

def generate_password(length=8):
    """
    Generate a random password.
    
    Args:
        length (int): Length of the password
        
    Returns:
        str: A random password
    """
    characters = string.ascii_letters + string.digits
    return ''.join(secrets.choice(characters) for _ in range(length))

def send_credentials_email(email, full_name, username, password):
    """
    Send account credentials email with the password and reset link.
    
    Args:
        email (str): Recipient email
        full_name (str): User's full name
        username (str): User's username
        password (str): User's generated password
    
    Returns:
        tuple: (success, message)
    """
    try:
        if not EMAIL_CONFIG['API_KEY'] or not EMAIL_CONFIG['API_SECRET']:
            raise ValueError("Mailjet API credentials are not configured")
            
        # Get reset password URL from environment variable
        reset_url = EMAIL_CONFIG['RESET_PASSWORD_URL']
            
        # Use Mailjet API
        mailjet = Client(auth=(EMAIL_CONFIG['API_KEY'], EMAIL_CONFIG['API_SECRET']), version='v3.1')
        
        # Create HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Welcome to ExamPortal</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 0;
                }}
                .container {{
                    background-color: #f9fafb;
                    border-radius: 8px;
                    overflow: hidden;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                    margin: 20px auto;
                }}
                .header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                    text-align: center;
                    border-radius: 8px 8px 0 0;
                }}
                .header h1 {{
                    margin: 0 0 10px 0;
                    font-weight: 600;
                    font-size: 24px;
                }}
                .header p {{
                    margin: 0;
                    opacity: 0.9;
                }}
                .content {{
                    background: #ffffff;
                    padding: 30px;
                    border-radius: 0 0 8px 8px;
                }}
                .credentials-box {{
                    background: #f3f4f6;
                    padding: 20px;
                    border-radius: 8px;
                    border-left: 4px solid #4f46e5;
                    margin: 20px 0;
                }}
                .credential-item {{
                    margin: 10px 0;
                    padding: 8px;
                    background: #ffffff;
                    border-radius: 4px;
                }}
                .credential-label {{
                    font-weight: 600;
                    color: #4b5563;
                    display: block;
                    margin-bottom: 5px;
                    font-size: 14px;
                }}
                .credential-value {{
                    color: #111827;
                    font-family: 'Courier New', monospace;
                    font-size: 16px;
                    padding: 8px 12px;
                    background-color: #f9fafb;
                    border-radius: 4px;
                    border: 1px solid #e5e7eb;
                }}
                .reset-link {{
                    display: block;
                    background: linear-gradient(135deg, #4f46e5 0%, #4338ca 100%);
                    color: white;
                    text-decoration: none;
                    padding: 16px 24px;
                    border-radius: 8px;
                    font-weight: 600;
                    text-align: center;
                    margin: 30px 0 15px;
                    transition: all 0.3s ease;
                }}
                .reset-link:hover {{
                    background: linear-gradient(135deg, #4338ca 0%, #3730a3 100%);
                }}
                .security-tips {{
                    margin-top: 30px;
                    padding: 15px;
                    background-color: #ecfdf5;
                    border-radius: 8px;
                    border-left: 4px solid #10b981;
                }}
                .security-tips h3 {{
                    color: #047857;
                    margin-top: 0;
                    font-size: 16px;
                }}
                .security-tips ul {{
                    padding-left: 20px;
                    margin-bottom: 0;
                    color: #065f46;
                }}
                .footer {{
                    text-align: center;
                    margin-top: 30px;
                    color: #6b7280;
                    font-size: 14px;
                    border-top: 1px solid #e5e7eb;
                    padding-top: 20px;
                }}
                .brand-highlight {{
                    color: #4f46e5;
                    font-weight: 600;
                }}
                .remember-note {{
                    background-color: #fffbeb;
                    padding: 15px;
                    border-radius: 8px;
                    border-left: 4px solid #f59e0b;
                    margin: 20px 0;
                }}
                .remember-note h3 {{
                    color: #b45309;
                    margin-top: 0;
                    font-size: 16px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Welcome to ExamPortal!</h1>
                    <p>Your account has been successfully created</p>
                </div>

                <div class="content">
                    <p>Dear <strong>{full_name}</strong>,</p>

                    <p>Thank you for creating an account with ExamPortal. Your account has been successfully set up and is ready to use.</p>

                    <div class="credentials-box">
                        <h3>Your Account Information</h3>
                        <div class="credential-item">
                            <span class="credential-label">Username:</span>
                            <div class="credential-value">{username}</div>
                        </div>
                        <div class="credential-item">
                            <span class="credential-label">Password:</span>
                            <div class="credential-value">{password}</div>
                        </div>
                        <div class="credential-item">
                            <span class="credential-label">Email:</span>
                            <div class="credential-value">{email}</div>
                        </div>
                    </div>
                    
                    <div class="remember-note">
                        <h3>Important</h3>
                        <p>Remember your password and please reset it after your first login for better security.</p>
                    </div>
                    
                    <a href="{reset_url}" class="reset-link">Reset Your Password</a>
                    
                    <div class="security-tips">
                        <h3>Security Tips</h3>
                        <ul>
                            <li>Please change your temporary password after your first login</li>
                            <li>Create a strong, unique password</li>
                            <li>Never share your login details with others</li>
                            <li>Log out when using shared computers</li>
                        </ul>
                    </div>

                    <p>If you have any questions or need assistance, please don't hesitate to contact our support team.</p>

                    <p>Best regards,<br>The ExamPortal Team</p>
                    
                    <div class="footer">
                        <p><strong class="brand-highlight">ExamPortal</strong></p>
                        <p>Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """

        # Create plain text content as fallback
        text_content = f"""
        Welcome to ExamPortal!

        Dear {full_name},

        Thank you for creating an account with ExamPortal. Your account has been successfully set up and is ready to use.

        Your Account Information:
        - Username: {username}
        - Password: {password}
        - Email: {email}

        IMPORTANT: Remember your password and please reset it after your first login for better security.
        
        You can reset your password at: {reset_url}

        Security Tips:
        - Please change your temporary password after your first login
        - Create a strong, unique password
        - Never share your login details with others
        - Log out when using shared computers

        If you have any questions or need assistance, please contact our support team.

        Best regards,
        The ExamPortal Team
        """

        # Prepare email data for Mailjet API
        data = {
            'Messages': [
                {
                    'From': {
                        'Email': EMAIL_CONFIG['FROM_EMAIL'],
                        'Name': EMAIL_CONFIG['FROM_NAME']
                    },
                    'To': [
                        {
                            'Email': email,
                            'Name': full_name
                        }
                    ],
                    'Subject': 'Welcome to ExamPortal - Your Account Information',
                    'TextPart': text_content,
                    'HTMLPart': html_content
                }
            ]
        }

        # Send email using Mailjet with detailed logging
        result = mailjet.send.create(data=data)
        

        
        if result.status_code == 200:
            print(f"Email successfully sent")
            return True, "Email sent successfully"
        else:
            print(f"Mailjet error: {result.json()}")
            return False, f"Failed to send email: API returned status {result.status_code}"

    except Exception as e:
        print(f"Error sending email: {e}")
        import traceback
        traceback.print_exc()
        return False, f"Failed to send email: {str(e)}"