import streamlit as st
import requests
from bs4 import BeautifulSoup
import os
import zipfile
import tempfile
import time
import re
import json
import shutil
from datetime import datetime
from typing import Dict, List, Optional

# Set page config
st.set_page_config(
    page_title="UK Company Registry Scraper",
    page_icon="🏢",
    layout="centered"
)

class HybridCompanyRegistryScraper:
    def __init__(self, company_id: str):
        self.company_id = company_id
        # Hardcoded API key
        self.api_key = "9e8a7289-ce2e-44c6-9e4f-45a36819a1aa"
        self.api_base = "https://api.companieshouse.gov.uk"
        self.web_base = "https://find-and-update.company-information.service.gov.uk"
        
        # API session with authentication
        self.api_session = requests.Session()
        self.api_session.auth = (self.api_key, '')
        self.api_session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'HybridCompanyExtractor/1.0'
        })
        
        # Web scraping session (only for PDFs)
        self.web_session = requests.Session()
        self.web_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive'
        })
        
        self.temp_dir = None
    
    def search_companies(self, search_term: str) -> List[Dict]:
        """Search for companies by name using the API"""
        try:
            url = f"{self.api_base}/search/companies"
            params = {
                'q': search_term,
                'items_per_page': 20
            }
            response = self.api_session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return data.get('items', [])
            else:
                st.error(f"Search API returned status {response.status_code}")
                
        except Exception as e:
            st.error(f"Error searching companies: {str(e)}")
        
        return []
        
    def create_temp_directory(self):
        """Create a temporary directory for storing files"""
        self.temp_dir = tempfile.mkdtemp(prefix=f"company_{self.company_id}_")
        return self.temp_dir
    
    def test_api_connection(self):
        """Test if API key is valid"""
        try:
            url = f"{self.api_base}/company/{self.company_id}"
            response = self.api_session.get(url, timeout=10)
            return response.status_code in [200, 404]
        except Exception:
            return False
    
    # === API METHODS (FAST & RELIABLE) ===
    
    def get_company_profile_api(self) -> Dict:
        """Get company overview using official API"""
        try:
            url = f"{self.api_base}/company/{self.company_id}"
            response = self.api_session.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                st.warning(f"Company {self.company_id} not found")
            else:
                st.warning(f"API returned status {response.status_code}")
        except Exception as e:
            st.error(f"Error fetching company profile: {str(e)}")
        return {}
    
    def get_officers_api(self) -> Dict:
        """Get officers data using official API"""
        try:
            url = f"{self.api_base}/company/{self.company_id}/officers"
            response = self.api_session.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            st.error(f"Error fetching officers data: {str(e)}")
        return {}
    
    def get_psc_api(self) -> Dict:
        """Get persons with significant control using official API"""
        try:
            url = f"{self.api_base}/company/{self.company_id}/persons-with-significant-control"
            response = self.api_session.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            st.error(f"Error fetching PSC data: {str(e)}")
        return {}
    
    def get_filing_history_api(self) -> Dict:
        """Get filing history metadata using official API"""
        try:
            url = f"{self.api_base}/company/{self.company_id}/filing-history"
            response = self.api_session.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            st.error(f"Error fetching filing history: {str(e)}")
        return {}
    
    # === FORMATTED OUTPUT METHODS ===
    
    def format_company_profile(self, data: Dict) -> str:
        """Format company profile data into readable text"""
        if not data:
            return "=== COMPANY OVERVIEW ===\n\nNo data available from API\n"
        
        text = "=== COMPANY OVERVIEW ===\n\n"
        text += f"Company Name: {data.get('company_name', 'N/A')}\n"
        text += f"Company Number: {data.get('company_number', 'N/A')}\n"
        text += f"Company Status: {data.get('company_status', 'N/A')}\n"
        text += f"Company Type: {data.get('type', 'N/A')}\n"
        
        # Registered office address
        if 'registered_office_address' in data:
            addr = data['registered_office_address']
            address_parts = []
            for field in ['address_line_1', 'address_line_2', 'locality', 'region', 'postal_code', 'country']:
                if field in addr and addr[field]:
                    address_parts.append(addr[field])
            text += f"Registered Office Address: {', '.join(address_parts)}\n"
        
        text += f"Incorporated On: {data.get('date_of_creation', 'N/A')}\n"
        text += f"Jurisdiction: {data.get('jurisdiction', 'N/A')}\n"
        
        # SIC codes
        if 'sic_codes' in data and data['sic_codes']:
            text += f"\nNature of Business (SIC):\n"
            for sic in data['sic_codes']:
                text += f"  {sic}\n"
        
        # Accounts information
        if 'accounts' in data:
            accounts = data['accounts']
            text += f"\n=== ACCOUNTS INFORMATION ===\n"
            text += f"Next Accounts Due: {accounts.get('next_due', 'N/A')}\n"
            text += f"Next Made Up To: {accounts.get('next_made_up_to', 'N/A')}\n"
            text += f"Last Accounts Made Up To: {accounts.get('last_accounts', {}).get('made_up_to', 'N/A')}\n"
        
        # Confirmation statement
        if 'confirmation_statement' in data:
            cs = data['confirmation_statement']
            text += f"\n=== CONFIRMATION STATEMENT ===\n"
            text += f"Next Statement Date: {cs.get('next_due', 'N/A')}\n"
            text += f"Next Made Up To: {cs.get('next_made_up_to', 'N/A')}\n"
            text += f"Last Made Up To: {cs.get('last_made_up_to', 'N/A')}\n"
        
        return text
    
    def format_officers(self, data: Dict) -> str:
        """Format officers data into readable text"""
        if not data or 'items' not in data:
            return "=== OFFICERS INFORMATION ===\n\nNo officers data available\n"
        
        text = "=== OFFICERS INFORMATION ===\n\n"
        text += f"Total Officers: {data.get('total_results', 0)}\n\n"
        
        for i, officer in enumerate(data.get('items', []), 1):
            text += f"Officer {i}:\n"
            text += f"  Name: {officer.get('name', 'N/A')}\n"
            text += f"  Role: {officer.get('officer_role', 'N/A')}\n"
            text += f"  Appointed On: {officer.get('appointed_on', 'N/A')}\n"
            
            if 'resigned_on' in officer:
                text += f"  Resigned On: {officer['resigned_on']}\n"
            
            if 'nationality' in officer:
                text += f"  Nationality: {officer['nationality']}\n"
            
            if 'country_of_residence' in officer:
                text += f"  Country of Residence: {officer['country_of_residence']}\n"
            
            if 'occupation' in officer:
                text += f"  Occupation: {officer['occupation']}\n"
            
            if 'date_of_birth' in officer:
                dob = officer['date_of_birth']
                text += f"  Date of Birth: {dob.get('month', '')}/{dob.get('year', '')}\n"
            
            # Address
            if 'address' in officer:
                addr = officer['address']
                address_parts = []
                for field in ['address_line_1', 'address_line_2', 'locality', 'region', 'postal_code', 'country']:
                    if field in addr and addr[field]:
                        address_parts.append(addr[field])
                text += f"  Address: {', '.join(address_parts)}\n"
            
            text += "\n" + "="*50 + "\n\n"
        
        return text
    
    def format_psc(self, data: Dict) -> str:
        """Format PSC data into readable text"""
        if not data or 'items' not in data:
            return "=== PERSONS WITH SIGNIFICANT CONTROL ===\n\nNo PSC data available\n"
        
        text = "=== PERSONS WITH SIGNIFICANT CONTROL ===\n\n"
        text += f"Total PSCs: {data.get('total_results', 0)}\n\n"
        
        for i, psc in enumerate(data.get('items', []), 1):
            text += f"PSC {i}:\n"
            text += f"  Name: {psc.get('name', 'N/A')}\n"
            text += f"  Kind: {psc.get('kind', 'N/A')}\n"
            text += f"  Notified On: {psc.get('notified_on', 'N/A')}\n"
            
            if 'nationality' in psc:
                text += f"  Nationality: {psc['nationality']}\n"
            
            if 'country_of_residence' in psc:
                text += f"  Country of Residence: {psc['country_of_residence']}\n"
            
            if 'date_of_birth' in psc:
                dob = psc['date_of_birth']
                text += f"  Date of Birth: {dob.get('month', '')}/{dob.get('year', '')}\n"
            
            # Nature of control
            if 'natures_of_control' in psc:
                text += f"  Nature of Control:\n"
                for control in psc['natures_of_control']:
                    text += f"    - {control}\n"
            
            # Address
            if 'address' in psc:
                addr = psc['address']
                address_parts = []
                for field in ['address_line_1', 'address_line_2', 'locality', 'region', 'postal_code', 'country']:
                    if field in addr and addr[field]:
                        address_parts.append(addr[field])
                text += f"  Address: {', '.join(address_parts)}\n"
            
            text += "\n" + "="*50 + "\n\n"
        
        return text
    
    def format_filing_history(self, data: Dict) -> str:
        """Format filing history data into readable text"""
        if not data or 'items' not in data:
            return "=== FILING HISTORY ===\n\nNo filing history available\n"
        
        text = "=== FILING HISTORY ===\n\n"
        text += f"Total Filings: {data.get('total_count', 0)}\n\n"
        
        for i, filing in enumerate(data.get('items', []), 1):
            text += f"Filing {i}:\n"
            text += f"  Date: {filing.get('date', 'N/A')}\n"
            text += f"  Description: {filing.get('description', 'N/A')}\n"
            text += f"  Category: {filing.get('category', 'N/A')}\n"
            text += f"  Type: {filing.get('type', 'N/A')}\n"
            
            if 'action_date' in filing:
                text += f"  Action Date: {filing['action_date']}\n"
            
            if 'pages' in filing:
                text += f"  Pages: {filing['pages']}\n"
            
            # Document links (metadata only from API)
            if 'links' in filing and 'document_metadata' in filing['links']:
                text += f"  Document Available: Yes\n"
            else:
                text += f"  Document Available: No\n"
            
            text += "\n" + "-"*40 + "\n\n"
        
        return text
    
    # === WEB SCRAPING METHODS (ONLY FOR PDFs) ===
    
    def get_pdf_links_scraping(self) -> List[str]:
        """Scrape PDF download links from filing history page"""
        try:
            url = f"{self.web_base}/company/{self.company_id}/filing-history"
            response = self.web_session.get(url, timeout=15)
            
            if response.status_code != 200:
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            pdf_links = []
            
            # Find all PDF links
            for link in soup.find_all('a', href=True):
                href = link['href']
                link_text = link.get_text().strip()
                
                # Look for PDF links or "View PDF" text
                if ('pdf' in href.lower() or 
                    'view pdf' in link_text.lower() or 
                    'document' in href.lower()):
                    
                    # Convert relative URLs to absolute
                    if href.startswith('/'):
                        href = 'https://find-and-update.company-information.service.gov.uk' + href
                    elif not href.startswith('http'):
                        continue
                    
                    pdf_links.append({
                        'url': href,
                        'description': link_text
                    })
            
            return pdf_links
            
        except Exception:
            return []
    
    def download_pdf(self, pdf_info: Dict, filename: str) -> bool:
        """Download a PDF file"""
        try:
            response = self.web_session.get(pdf_info['url'], timeout=30)
            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)
                return True
        except Exception:
            pass
        return False
    
    # === MAIN EXTRACTION METHOD ===
    
    def create_zip_file(self):
        """Create a ZIP file containing all scraped information and PDFs using hybrid approach"""
        if not self.temp_dir:
            self.create_temp_directory()
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Test API connection first
        status_text.text("Testing API connection...")
        if not self.test_api_connection():
            st.error("❌ Cannot connect to Companies House API. Please check connection.")
            return None, 0
        
        status_text.text("✅ API connection successful!")
        progress_bar.progress(10)
        
        # Use API for structured data (fast & reliable)
        status_text.text("📡 Fetching company profile...")
        progress_bar.progress(20)
        company_profile = self.get_company_profile_api()
        
        status_text.text("📡 Fetching officers data...")
        progress_bar.progress(35)
        officers_data = self.get_officers_api()
        
        # Rate limiting: API allows 2 requests per second
        time.sleep(0.6)
        
        status_text.text("📡 Fetching PSC data...")
        progress_bar.progress(50)
        psc_data = self.get_psc_api()
        
        status_text.text("📡 Fetching filing history...")
        progress_bar.progress(65)
        filing_history_data = self.get_filing_history_api()
        
        # Rate limiting
        time.sleep(0.6)
        
        # Use web scraping ONLY for PDF downloads
        status_text.text("🕷️ Finding PDF documents...")
        progress_bar.progress(75)
        pdf_links = self.get_pdf_links_scraping()
        
        status_text.text(f"📥 Downloading {len(pdf_links)} PDF documents...")
        progress_bar.progress(80)
        
        pdf_files = []
        pdf_log = "\n=== PDF DOCUMENTS ===\n\n"
        
        for i, pdf_info in enumerate(pdf_links):
            try:
                filename = f"filing_document_{i+1}_{self.company_id}.pdf"
                pdf_path = os.path.join(self.temp_dir, filename)
                
                if self.download_pdf(pdf_info, pdf_path):
                    pdf_files.append(pdf_path)
                    pdf_log += f"✅ Downloaded: {filename}\n"
                else:
                    pdf_log += f"❌ Failed: {pdf_info.get('description', 'Unknown')}\n"
                
                # Be respectful with scraping
                time.sleep(1)
                
            except Exception:
                pdf_log += f"❌ Error downloading document {i+1}\n"
        
        status_text.text("📄 Creating files...")
        progress_bar.progress(90)
        
        # Format and save text files
        files_created = []
        
        # Company profile
        overview_text = self.format_company_profile(company_profile)
        overview_path = os.path.join(self.temp_dir, f"{self.company_id}_overview.txt")
        with open(overview_path, 'w', encoding='utf-8') as f:
            f.write(overview_text)
        files_created.append(overview_path)
        
        # Officers
        officers_text = self.format_officers(officers_data)
        officers_path = os.path.join(self.temp_dir, f"{self.company_id}_officers.txt")
        with open(officers_path, 'w', encoding='utf-8') as f:
            f.write(officers_text)
        files_created.append(officers_path)
        
        # PSC
        psc_text = self.format_psc(psc_data)
        psc_path = os.path.join(self.temp_dir, f"{self.company_id}_psc.txt")
        with open(psc_path, 'w', encoding='utf-8') as f:
            f.write(psc_text)
        files_created.append(psc_path)
        
        # Filing history
        filing_text = self.format_filing_history(filing_history_data) + pdf_log
        filing_path = os.path.join(self.temp_dir, f"{self.company_id}_filing_history.txt")
        with open(filing_path, 'w', encoding='utf-8') as f:
            f.write(filing_text)
        files_created.append(filing_path)
        
        # Create summary file
        summary_text = f"=== COMPANY DATA EXTRACTION SUMMARY ===\n\n"
        summary_text += f"Company ID: {self.company_id}\n"
        summary_text += f"Extraction Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        summary_text += f"Method: Hybrid (Companies House API + Web Scraping)\n\n"
        summary_text += f"Files Created: {len(files_created)} text files\n"
        summary_text += f"PDFs Downloaded: {len(pdf_files)}\n"
        
        summary_path = os.path.join(self.temp_dir, f"{self.company_id}_summary.txt")
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(summary_text)
        files_created.append(summary_path)
        
        # Create ZIP file
        zip_filename = f"company_{self.company_id}_data.zip"
        zip_path = os.path.join(self.temp_dir, zip_filename)
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add text files
            for file_path in files_created:
                zipf.write(file_path, os.path.basename(file_path))
            
            # Add PDF files
            for pdf_path in pdf_files:
                zipf.write(pdf_path, os.path.basename(pdf_path))
        
        progress_bar.progress(100)
        status_text.text("✅ Complete!")
        
        return zip_path, len(files_created) + len(pdf_files)
    
    def cleanup(self):
        """Clean up temporary directory and files"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

# Initialize session state
if 'search_results' not in st.session_state:
    st.session_state.search_results = []
if 'search_term' not in st.session_state:
    st.session_state.search_term = ""
if 'extraction_in_progress' not in st.session_state:
    st.session_state.extraction_in_progress = False

# Streamlit UI - COMPLETE FIXED VERSION
def main():
    st.title("🏢 UK Company Registry Scraper")
    
    # If extraction is in progress, show only extraction interface
    if st.session_state.get('extraction_in_progress', False):
        company_id = st.session_state.get('selected_company', '')
        
        st.info(f"Extracting data for company: {company_id}")
        
        try:
            scraper = HybridCompanyRegistryScraper(company_id)
            
            with st.spinner("Initializing..."):
                scraper.create_temp_directory()
            
            # Create the ZIP file with all data
            zip_path, file_count = scraper.create_zip_file()
            
            if zip_path and os.path.exists(zip_path):
                # Read the ZIP file for download
                with open(zip_path, 'rb') as zip_file:
                    zip_data = zip_file.read()
                
                st.success(f"✅ Successfully extracted company {company_id}!")
                st.info(f"📦 Package contains {file_count} files")
                
                # Download button
                st.download_button(
                    label="📥 Download Company Data ZIP",
                    data=zip_data,
                    file_name=f"company_{company_id}_data.zip",
                    mime="application/zip"
                )
                
                # Cleanup after successful download
                scraper.cleanup()
                
                # Reset buttons
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🔄 Search Another Company"):
                        st.session_state.extraction_in_progress = False
                        st.session_state.selected_company = None
                        st.rerun()
                
                with col2:
                    if st.button("🔍 New Search"):
                        st.session_state.extraction_in_progress = False
                        st.session_state.selected_company = None
                        st.session_state.search_results = []
                        st.session_state.search_term = ""
                        st.rerun()
                
            else:
                st.error("Failed to create the data package. Please try again.")
                scraper.cleanup()
                st.session_state.extraction_in_progress = False
                
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.session_state.extraction_in_progress = False
            try:
                scraper.cleanup()
            except:
                pass
        
        return
    
    # Create tabs for Search and Direct Entry
    tab1, tab2 = st.tabs(["🔍 Search Companies", "📝 Direct Entry"])
    
    with tab1:
        st.subheader("Search by Company Name")
        search_term = st.text_input(
            "Enter company name to search:",
            placeholder="e.g., Marks and Spencer, Tesco, Apple",
            value=st.session_state.search_term
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔍 Search Companies"):
                if search_term:
                    with st.spinner("Searching companies..."):
                        # Create a temporary scraper just for search
                        temp_scraper = HybridCompanyRegistryScraper("temp")
                        search_results = temp_scraper.search_companies(search_term)
                        
                        # Store results in session state
                        st.session_state.search_results = search_results
                        st.session_state.search_term = search_term
                        st.rerun()
                else:
                    st.error("Please enter a search term!")
        
        with col2:
            if st.button("🔄 Clear Results"):
                st.session_state.search_results = []
                st.session_state.search_term = ""
                st.rerun()
        
        # Display search results from session state
        if st.session_state.search_results:
            st.success(f"Found {len(st.session_state.search_results)} companies:")
            
            # Display search results
            for i, company in enumerate(st.session_state.search_results):
                company_name = company.get('title', 'Unknown Company')
                company_number = company.get('company_number', 'Unknown')
                company_status = company.get('company_status', 'Unknown')
                address = company.get('address_snippet', 'No address available')
                
                # Create a container for each result
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**{company_name}**")
                        st.write(f"Company Number: {company_number}")
                        st.write(f"Status: {company_status}")
                        st.write(f"Address: {address}")
                    
                    with col2:
                        # Extract button for this specific company
                        if st.button(f"Extract Data", key=f"extract_{i}"):
                            st.session_state.selected_company = company_number
                            st.session_state.extraction_in_progress = True
                            st.rerun()
                    
                    st.divider()
        
        elif st.session_state.search_term and not st.session_state.search_results:
            st.warning("No companies found with that search term. Try a different search.")
    
    with tab2:
        st.subheader("Enter Company Number Directly")
        company_id = st.text_input(
            "Enter Company Number:",
            placeholder="e.g., 15877621, 00006245, SC534841"
        )
        
        if st.button("🔍 Extract Company Data"):
            if company_id:
                st.session_state.selected_company = company_id
                st.session_state.extraction_in_progress = True
                st.rerun()
            else:
                st.error("Please enter a company number!")

if __name__ == "__main__":
    main()
