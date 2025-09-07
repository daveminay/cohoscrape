import streamlit as st
import requests
from bs4 import BeautifulSoup
import os
import zipfile
import tempfile
import time
import shutil
from datetime import datetime
from typing import List, Dict

st.set_page_config(
    page_title="UK Company Registry Scraper",
    page_icon="🏢",
    layout="centered"
)

ADMIN_PASSWORD = os.getenv('SCRAPER_PASSWORD')

def check_password():
    def password_entered():
        if ADMIN_PASSWORD and st.session_state.get("password") == ADMIN_PASSWORD:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    if st.session_state.get("password_correct", False):
        return True
    st.markdown("## 🔐 UK Company Registry Scraper")
    st.markdown("### Access Authentication Required")
    if not ADMIN_PASSWORD:
        st.error("🚨 System Error: SCRAPER_PASSWORD environment variable not configured!")
        st.stop()
    st.text_input(
        "Password:",
        type="password",
        on_change=password_entered,
        key="password",
        placeholder="Enter your access password"
    )
    if "password_correct" in st.session_state and not st.session_state["password_correct"]:
        st.error("❌ Password incorrect. Please try again.")
    st.markdown("---")
    st.markdown("*Secure access to UK Companies House data extraction tools.*")
    return False

def logout():
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

if 'search_results' not in st.session_state:
    st.session_state.search_results = []
if 'search_term' not in st.session_state:
    st.session_state.search_term = ""
if 'extraction_in_progress' not in st.session_state:
    st.session_state.extraction_in_progress = False
if 'extraction_complete' not in st.session_state:
    st.session_state.extraction_complete = False
if 'zip_data' not in st.session_state:
    st.session_state.zip_data = None
if 'file_count' not in st.session_state:
    st.session_state.file_count = 0
if 'last_search_term' not in st.session_state:
    st.session_state.last_search_term = ""

class HybridCompanyRegistryScraper:
    def __init__(self, company_id: str):
        self.company_id = company_id
        self.api_key = os.environ.get('COMPANIES_HOUSE_API_KEY')
        if not self.api_key:
            st.error("🚨 System Error: COMPANIES_HOUSE_API_KEY environment variable not set!")
            st.stop()
        self.api_base = "https://api.companieshouse.gov.uk"
        self.web_base = "https://find-and-update.company-information.service.gov.uk"

        self.api_session = requests.Session()
        self.api_session.auth = (self.api_key, '')
        self.api_session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'HybridCompanyExtractor/1.0'
        })

        self.web_session = requests.Session()
        self.web_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive'
        })

        self.temp_dir = None

    def search_companies(self, search_term: str) -> List[Dict]:
        try:
            url = f"{self.api_base}/search/companies"
            params = {'q': search_term, 'items_per_page': 20}
            response = self.api_session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json().get('items', [])
            else:
                st.error(f"Search API returned status {response.status_code}")
        except Exception as e:
            st.error(f"Error searching companies: {str(e)}")
        return []

    def create_temp_directory(self):
        self.temp_dir = tempfile.mkdtemp(prefix=f"company_{self.company_id}_")
        return self.temp_dir

    def test_api_connection(self):
        try:
            url = f"{self.api_base}/company/{self.company_id}"
            response = self.api_session.get(url, timeout=10)
            return response.status_code in [200, 404]
        except Exception:
            return False

    def get_company_profile_api(self) -> Dict:
        try:
            url = f"{self.api_base}/company/{self.company_id}"
            response = self.api_session.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                st.warning(f"Company {self.company_id} not found")
        except Exception as e:
            st.error(f"Error fetching company profile: {str(e)}")
        return {}

    def get_officers_api(self) -> Dict:
        try:
            url = f"{self.api_base}/company/{self.company_id}/officers"
            response = self.api_session.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return {}

    def get_psc_api(self) -> Dict:
        try:
            url = f"{self.api_base}/company/{self.company_id}/persons-with-significant-control"
            response = self.api_session.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return {}

    def get_filing_history_api(self) -> Dict:
        try:
            url = f"{self.api_base}/company/{self.company_id}/filing-history"
            response = self.api_session.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return {}

    def get_pdf_links_scraping(self) -> List[Dict]:
        all_pdf_links = []
        page_num = 1
        max_pages = 20
        try:
            while page_num <= max_pages:
                if page_num == 1:
                    url = f"{self.web_base}/company/{self.company_id}/filing-history"
                else:
                    url = f"{self.web_base}/company/{self.company_id}/filing-history?page={page_num}"
                response = self.web_session.get(url, timeout=15)
                if response.status_code != 200:
                    break
                soup = BeautifulSoup(response.text, 'html.parser')
                page_pdf_links = []
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    text = link.get_text().strip()
                    if 'pdf' in href.lower() or 'view pdf' in text.lower() or 'document' in href.lower():
                        if href.startswith('/'):
                            href = 'https://find-and-update.company-information.service.gov.uk' + href
                        elif not href.startswith('http'):
                            continue
                        page_pdf_links.append({'url': href, 'description': text, 'page': page_num})
                if not page_pdf_links:
                    break
                all_pdf_links.extend(page_pdf_links)
                has_next_page = False
                pagination_links = soup.find_all('a', href=True)
                for pag_link in pagination_links:
                    href = pag_link.get('href', '')
                    text = pag_link.get_text().strip().lower()
                    if ('next' in text or 
                        f'page={page_num + 1}' in href or
                        (text.isdigit() and int(text) > page_num)):
                        has_next_page = True
                        break
                if not has_next_page:
                    for pag_link in soup.find_all('a', href=True):
                        if f'page={page_num + 1}' in pag_link.get('href', ''):
                            has_next_page = True
                            break
                if not has_next_page:
                    break
                page_num += 1
                time.sleep(1)
            return all_pdf_links
        except Exception:
            return all_pdf_links

    def download_pdf(self, pdf_info: Dict, filename: str) -> bool:
        try:
            response = self.web_session.get(pdf_info['url'], timeout=30)
            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)
                return True
        except Exception:
            pass
        return False

    def format_company_profile(self, data: Dict) -> str:
        if not data:
            return "=== COMPANY OVERVIEW ===\n\nNo data available from API\n"
        text = "=== COMPANY OVERVIEW ===\n\n"
        text += f"Company Name: {data.get('company_name', 'N/A')}\n"
        text += f"Company Number: {data.get('company_number', 'N/A')}\n"
        text += f"Company Status: {data.get('company_status', 'N/A')}\n"
        text += f"Company Type: {data.get('type', 'N/A')}\n"
        text += f"Incorporated On: {data.get('date_of_creation', 'N/A')}\n"
        text += f"Jurisdiction: {data.get('jurisdiction', 'N/A')}\n"
        if 'registered_office_address' in data:
            addr = data['registered_office_address']
            address_parts = []
            for field in ['address_line_1', 'address_line_2', 'locality', 'region', 'postal_code', 'country']:
                if field in addr and addr[field]:
                    address_parts.append(addr[field])
            text += f"Registered Office Address: {', '.join(address_parts)}\n"
        if 'sic_codes' in data and data['sic_codes']:
            text += "\nNature of Business (SIC):\n"
            for sic in data['sic_codes']:
                text += f"  {sic}\n"
        if 'accounts' in data:
            accounts = data['accounts']
            text += "\n=== ACCOUNTS INFORMATION ===\n"
            text += f"Next Accounts Due: {accounts.get('next_due', 'N/A')}\n"
            text += f"Next Made Up To: {accounts.get('next_made_up_to', 'N/A')}\n"
            text += f"Last Accounts Made Up To: {accounts.get('last_accounts', {}).get('made_up_to', 'N/A')}\n"
        if 'confirmation_statement' in data:
            cs = data['confirmation_statement']
            text += "\n=== CONFIRMATION STATEMENT ===\n"
            text += f"Next Statement Date: {cs.get('next_due', 'N/A')}\n"
            text += f"Next Made Up To: {cs.get('next_made_up_to', 'N/A')}\n"
            text += f"Last Made Up To: {cs.get('last_made_up_to', 'N/A')}\n"
        return text

    def format_officers(self, data: Dict) -> str:
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
            if 'natures_of_control' in psc:
                text += "  Nature of Control:\n"
                for control in psc['natures_of_control']:
                    text += f"    - {control}\n"
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
        if not data or 'items' not in data:
            return "=== FILING HISTORY ===\n\nNo filing history available\n"
        text = "=== FILING HISTORY ===\n\n"
        text += f"Total Filings: {data.get('total_count', 0)}\n\n"
        for i, filing in enumerate(data.get('items', [])[:15], 1):
            text += f"Filing {i}:\n"
            text += f"  Date: {filing.get('date', 'N/A')}\n"
            text += f"  Description: {filing.get('description', 'N/A')}\n"
            text += f"  Category: {filing.get('category', 'N/A')}\n"
            text += f"  Type: {filing.get('type', 'N/A')}\n"
            if 'action_date' in filing:
                text += f"  Action Date: {filing['action_date']}\n"
            if 'pages' in filing:
                text += f"  Pages: {filing['pages']}\n"
            if 'links' in filing and 'document_metadata' in filing['links']:
                text += f"  Document Available: Yes\n"
            else:
                text += f"  Document Available: No\n"
            text += "\n" + "-"*40 + "\n\n"
        return text

    def create_zip_file(self):
        if not self.temp_dir:
            self.create_temp_directory()
        progress_bar = st.progress(0)
        status_text = st.empty()

        status_text.text("Testing API connection...")
        if not self.test_api_connection():
            st.error("❌ Cannot connect to Companies House API. Please check connection.")
            return None, 0

        status_text.text("✅ API connection successful!")
        progress_bar.progress(10)

        status_text.text("📡 Fetching company profile...")
        company_profile = self.get_company_profile_api()
        progress_bar.progress(25)

        status_text.text("📡 Fetching officers data...")
        officers_data = self.get_officers_api()
        progress_bar.progress(40)

        time.sleep(0.6)  # Rate limiting

        status_text.text("📡 Fetching PSC data...")
        psc_data = self.get_psc_api()
        progress_bar.progress(55)

        status_text.text("📡 Fetching filing history...")
        filing_history_data = self.get_filing_history_api()
        progress_bar.progress(70)

        time.sleep(0.6)  # Rate limiting

        status_text.text("🕷️ Finding PDF documents across all pages...")
        pdf_links = self.get_pdf_links_scraping()
        progress_bar.progress(80)
        status_text.text(f"Found {len(pdf_links)} PDF documents across all pages")

        status_text.text(f"📥 Downloading {len(pdf_links)} PDF documents...")
        pdf_files = []
        pdf_log = "\n=== PDF DOCUMENTS ===\n\n"

        for i, pdf_info in enumerate(pdf_links):
            filename = f"filing_document_{i+1}_{self.company_id}.pdf"
            pdf_path = os.path.join(self.temp_dir, filename)
            if self.download_pdf(pdf_info, pdf_path):
                pdf_files.append(pdf_path)
                pdf_log += f"✅ Downloaded: {filename} (Page {pdf_info.get('page', 'Unknown')})\n"
            else:
                pdf_log += f"❌ Failed: {pdf_info.get('description', 'Unknown')} (Page {pdf_info.get('page', 'Unknown')})\n"
            time.sleep(1)  # Polite delay

        progress_bar.progress(90)
        status_text.text("📄 Creating files...")

        files_created = []

        overview_text = self.format_company_profile(company_profile)
        overview_path = os.path.join(self.temp_dir, f"{self.company_id}_overview.txt")
        with open(overview_path, 'w', encoding='utf-8') as f:
            f.write(overview_text)
        files_created.append(overview_path)

        officers_text = self.format_officers(officers_data)
        officers_path = os.path.join(self.temp_dir, f"{self.company_id}_officers.txt")
        with open(officers_path, 'w', encoding='utf-8') as f:
            f.write(officers_text)
        files_created.append(officers_path)

        psc_text = self.format_psc(psc_data)
        psc_path = os.path.join(self.temp_dir, f"{self.company_id}_psc.txt")
        with open(psc_path, 'w', encoding='utf-8') as f:
            f.write(psc_text)
        files_created.append(psc_path)

        filing_text = self.format_filing_history(filing_history_data) + pdf_log
        filing_path = os.path.join(self.temp_dir, f"{self.company_id}_filing_history.txt")
        with open(filing_path, 'w', encoding='utf-8') as f:
            f.write(filing_text)
        files_created.append(filing_path)

        summary_text = f"=== COMPANY DATA EXTRACTION SUMMARY ===\n\n"
        summary_text += f"Company ID: {self.company_id}\n"
        summary_text += f"Extraction Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        summary_text += f"Method: Hybrid (Companies House API + Multi-Page Web Scraping)\n\n"
        summary_text += f"Files Created: {len(files_created)} text files\n"
        summary_text += f"PDFs Downloaded: {len(pdf_files)}\n"
        summary_text += f"Pages Scraped: {max([pdf.get('page', 1) for pdf in pdf_links]) if pdf_links else 1}\n"

        summary_path = os.path.join(self.temp_dir, f"{self.company_id}_summary.txt")
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(summary_text)
        files_created.append(summary_path)

        zip_filename = f"company_{self.company_id}_data.zip"
        zip_path = os.path.join(self.temp_dir, zip_filename)

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_created:
                zipf.write(file_path, os.path.basename(file_path))
            for pdf_path in pdf_files:
                zipf.write(pdf_path, os.path.basename(pdf_path))

        progress_bar.progress(100)
        status_text.text("✅ Complete!")

        return zip_path, len(files_created) + len(pdf_files)

    def cleanup(self):
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

def main():
    if not check_password():
        return

    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("🏢 UK Company Registry Scraper")
    with col2:
        if st.button("🚪 Logout"):
            logout()

    st.markdown("---")

    if st.session_state.get('extraction_in_progress', False):
        company_id = st.session_state.get('selected_company', '')

        # Check if extraction is already complete
        if st.session_state.get('extraction_complete', False):
            st.success(f"✅ Successfully extracted company {company_id}!")
            st.info(f"📦 Package contains {st.session_state.file_count} files")
            st.download_button(
                label="📥 Download Company Data ZIP",
                data=st.session_state.zip_data,
                file_name=f"company_{company_id}_data.zip",
                mime="application/zip"
            )

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Start Again"):
                    # Clear all session state except password authentication
                    keys_to_keep = ['password_correct']
                    keys_to_delete = [key for key in st.session_state.keys() if key not in keys_to_keep]
                    for key in keys_to_delete:
                        del st.session_state[key]
                    # Re-initialize required session state variables
                    st.session_state.search_results = []
                    st.session_state.search_term = ""
                    st.session_state.extraction_in_progress = False
                    st.session_state.extraction_complete = False
                    st.session_state.zip_data = None
                    st.session_state.file_count = 0
                    st.session_state.last_search_term = ""
                    st.rerun()
            with col2:
                if st.button("Logout"):
                    logout()
        else:
            st.info(f"Extracting data for company: {company_id}")
            try:
                scraper = HybridCompanyRegistryScraper(company_id)
                scraper.create_temp_directory()
                zip_path, file_count = scraper.create_zip_file()
                if zip_path and os.path.exists(zip_path):
                    with open(zip_path, 'rb') as zip_file:
                        zip_data = zip_file.read()
                    # Store results in session state
                    st.session_state.zip_data = zip_data
                    st.session_state.file_count = file_count
                    st.session_state.extraction_complete = True
                    scraper.cleanup()
                    st.rerun()
                else:
                    st.error("Failed to create the data package. Please try again.")
                    st.session_state.extraction_in_progress = False

            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.session_state.extraction_in_progress = False
                try:
                    scraper.cleanup()
                except:
                    pass
        return

    tab1, tab2 = st.tabs(["🔍 Search Companies", "📝 Direct Entry"])

    with tab1:
        st.subheader("Search by Company Name")
        search_term = st.text_input(
            "Enter company name to search:",
            placeholder="e.g., Tesco, Apple, Microsoft",
            value=st.session_state.search_term
        )
        st.session_state.last_search_term = search_term

        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔍 Search Companies"):
                if search_term:
                    with st.spinner("Searching companies..."):
                        temp_scraper = HybridCompanyRegistryScraper("temp")
                        search_results = temp_scraper.search_companies(search_term)
                        st.session_state.search_results = search_results
                        st.session_state.search_term = search_term
                        st.session_state.last_search_term = search_term
                        st.rerun()
                else:
                    st.error("Please enter a search term!")
        with col2:
            if st.button("🔄 Clear Results"):
                st.session_state.search_results = []
                st.session_state.search_term = ""
                st.session_state.last_search_term = ""
                st.rerun()
        if st.session_state.search_results:
            st.success(f"Found {len(st.session_state.search_results)} companies:")
            for i, company in enumerate(st.session_state.search_results):
                company_name = company.get('title', 'Unknown Company')
                company_number = company.get('company_number', 'Unknown')
                company_status = company.get('company_status', 'Unknown')
                address = company.get('address_snippet', 'No address available')
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.write(f"**{company_name}**")
                        st.write(f"Company Number: {company_number}")
                        st.write(f"Status: {company_status}")
                        st.write(f"Address: {address}")
                    with col2:
                        if st.button("Extract Data", key=f"extract_{i}"):
                            st.session_state.selected_company = company_number
                            st.session_state.extraction_in_progress = True
                            st.session_state.extraction_complete = False
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
                st.session_state.extraction_complete = False
                st.rerun()
            else:
                st.error("Please enter a company number!")

if __name__ == "__main__":
    main()
