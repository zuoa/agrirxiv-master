import os
import time
import json
import csv
import random
from urllib.parse import urljoin, urlparse, parse_qs
from pathlib import Path
import re

import requests
from bs4 import BeautifulSoup
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CABDigitalLibraryCrawler:
    def __init__(self, base_url="https://www.cabidigitallibrary.org"):
        self.base_url = base_url
        self.session = requests.Session()

        # 随机选择User-Agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
        ]

        # 设置请求头，模拟真实浏览器访问
        self.session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        })
        self.articles_data = []

    def _is_captcha_page(self, soup):
        """检查是否为验证页面"""
        captcha_indicators = [
            'captcha', 'verification', 'challenge', 'robot', 'cloudflare'
        ]
        try:
            page_text = soup.get_text().lower()
            return any(indicator in page_text for indicator in captcha_indicators)
        except Exception:
            return False

    def get_search_results(self, search_url, max_pages=None):
        """
        获取搜索结果页面的所有文章信息

        Args:
            search_url (str): 搜索页面URL
            max_pages (int): 最大页数限制，None为无限制

        Returns:
            list: 文章信息列表
        """
        articles = []
        page = 0
        retry_count = 0
        max_retries = 3

        # 首先访问主页建立会话
        try:
            logger.info("建立会话连接...")
            self.session.get(self.base_url, timeout=30)
            time.sleep(2)
        except Exception as e:
            logger.warning(f"无法访问主页: {e}")

        while True:
            # 构建当前页面URL
            if page == 0:
                current_url = search_url
            else:
                # 修改URL中的startPage参数
                current_url = re.sub(r'startPage=\d+', f'startPage={page * 20}', search_url)

            logger.info(f"正在爬取第 {page + 1} 页: {current_url}")

            try:
                # 随机延迟
                delay = random.uniform(3, 8)
                time.sleep(delay)

                # 随机更换User-Agent
                if random.random() < 0.3:  # 30%概率更换
                    self._update_user_agent()

                # 添加Referer头
                if page > 0:
                    self.session.headers['Referer'] = search_url

                response = self.session.get(current_url, timeout=30)

                # 检查响应状态
                if response.status_code == 403:
                    logger.warning("遇到403错误，尝试重新建立会话...")
                    self._reset_session()
                    time.sleep(random.uniform(10, 20))
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error("多次重试失败，停止爬取")
                        break
                    continue

                response.raise_for_status()
                retry_count = 0  # 成功后重置重试计数

                # 处理编码问题
                response.encoding = self._detect_encoding(response)

                # 解析HTML
                soup = BeautifulSoup(response.text, 'html.parser')

                # 检查是否被重定向到验证页面
                if self._is_captcha_page(soup):
                    logger.error("遇到验证页面，请手动访问网站完成验证后重试")
                    break

                # 查找文章列表
                article_items = self._extract_articles_from_page(soup)

                if not article_items:
                    logger.info("没有找到更多文章，停止爬取")
                    break

                articles.extend(article_items)
                logger.info(f"第 {page + 1} 页找到 {len(article_items)} 篇文章")

                # 检查是否还有下一页
                if not self._has_next_page(soup):
                    logger.info("已到达最后一页")
                    break

                # 检查页数限制
                if max_pages and page + 1 >= max_pages:
                    logger.info(f"已达到最大页数限制: {max_pages}")
                    break

                page += 1

            except Exception as e:
                logger.error(f"爬取第 {page + 1} 页时出错: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error("达到最大重试次数，停止爬取")
                    break
                time.sleep(random.uniform(5, 15))

        logger.info(f"总共找到 {len(articles)} 篇文章")
        self.articles_data = articles
        return articles

    def _update_user_agent(self):
        """更新User-Agent"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
        ]
        self.session.headers['User-Agent'] = random.choice(user_agents)

    def _reset_session(self):
        """重置会话"""
        self.session.close()
        self.session = requests.Session()
        self.__init__(self.base_url)
        logger.info("会话已重置")

    def _detect_encoding(self, response):
        """检测并设置正确的编码"""
        # 1. 优先使用响应头中的编码
        if response.encoding and response.encoding.lower() != 'iso-8859-1':
            return response.encoding

        # 2. 尝试从HTML meta标签中获取编码
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')

            # 查找charset meta标签
            meta_charset = soup.find('meta', attrs={'charset': True})
            if meta_charset:
                return meta_charset['charset']

            # 查找http-equiv meta标签
            meta_http_equiv = soup.find('meta', attrs={'http-equiv': 'Content-Type'})
            if meta_http_equiv and meta_http_equiv.get('content'):
                import re
                charset_match = re.search(r'charset=([^;]+)', meta_http_equiv['content'])
                if charset_match:
                    return charset_match.group(1).strip()
        except:
            pass

        # 3. 尝试常见编码
        for encoding in ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']:
            try:
                response.content.decode(encoding)
                return encoding
            except (UnicodeDecodeError, LookupError):
                continue

        # 4. 默认使用utf-8，忽略错误
        return 'utf-8'

    def _extract_articles_from_page(self, soup):
        """从页面中提取文章信息"""
        articles = []

        # 查找文章条目 - 需要根据实际HTML结构调整选择器
        article_elements = soup.find_all(['div', 'article'], class_=re.compile(r'(item|result|article|entry)', re.I))

        if not article_elements:
            # 尝试其他可能的选择器
            article_elements = soup.find_all('div', {'data-doi': True}) or \
                               soup.find_all('div', class_=re.compile(r'search-result', re.I)) or \
                               soup.find_all('li', class_=re.compile(r'result', re.I))

        for element in article_elements:
            try:
                article_info = self._extract_article_info(element)
                if article_info:
                    articles.append(article_info)
            except Exception as e:
                logger.warning(f"提取文章信息时出错: {e}")
                continue

        # 如果上述方法都没找到，尝试查找所有包含DOI或PDF链接的元素
        if not articles:
            potential_articles = soup.find_all('a', href=re.compile(r'(doi|pdf)', re.I))
            for link in potential_articles:
                parent = link.find_parent(['div', 'li', 'article'])
                if parent:
                    try:
                        article_info = self._extract_article_info(parent)
                        if article_info and article_info not in articles:
                            articles.append(article_info)
                    except:
                        continue

        return articles

    def _extract_article_info(self, element):
        """从单个文章元素中提取信息"""
        article_info = {
            'title': '',
            'authors': '',
            'publication_date': '',
            'doi': '',
            'abstract': '',
            'pdf_url': '',
            'article_url': '',
            'journal': '',
            'volume': '',
            'issue': '',
            'pages': ''
        }

        try:
            # 提取标题
            title_elem = element.find(['h1', 'h2', 'h3', 'h4'], class_=re.compile(r'title', re.I)) or \
                         element.find('a', class_=re.compile(r'title', re.I)) or \
                         element.find(['h1', 'h2', 'h3', 'h4']) or \
                         element.find('span', class_=re.compile(r'title', re.I))

            if title_elem:
                article_info['title'] = self._safe_get_text(title_elem)

            # 提取作者
            authors_elem = element.find(['span', 'div'], class_=re.compile(r'author', re.I)) or \
                           element.find(['span', 'div'], class_=re.compile(r'contrib', re.I))

            if authors_elem:
                article_info['authors'] = self._safe_get_text(authors_elem)

            # 提取DOI
            doi_link = element.find('a', href=re.compile(r'/doi/', re.I))
            if doi_link:
                href = doi_link.get('href')
                article_info['article_url'] = urljoin(self.base_url, href)
                # 从URL中提取DOI
                doi_match = re.search(r'/doi/(?:abs/|full/)?(.+)', href)
                if doi_match:
                    article_info['doi'] = doi_match.group(1)

            # 查找PDF链接
            pdf_link = element.find('a', href=re.compile(r'pdf', re.I)) or \
                       element.find('a', string=re.compile(r'pdf', re.I))

            if pdf_link:
                pdf_href = pdf_link.get('href')
                article_info['pdf_url'] = urljoin(self.base_url, pdf_href)
            elif article_info['article_url']:
                # 尝试构建PDF URL
                pdf_url = article_info['article_url'].replace('/doi/abs/', '/doi/pdf/').replace('/doi/full/', '/doi/pdf/')
                if '/doi/pdf/' in pdf_url:
                    article_info['pdf_url'] = pdf_url

            # 提取发布日期
            date_elem = element.find(['span', 'div'], class_=re.compile(r'date|published', re.I))
            if date_elem:
                article_info['publication_date'] = self._safe_get_text(date_elem)

            # 提取摘要
            abstract_elem = element.find(['div', 'p'], class_=re.compile(r'abstract', re.I))
            if abstract_elem:
                abstract_text = self._safe_get_text(abstract_elem)
                article_info['abstract'] = abstract_text[:500] if abstract_text else ''  # 限制长度

            # 只有当至少有标题时才返回文章信息
            if article_info['title']:
                return article_info

        except Exception as e:
            logger.warning(f"提取文章信息时出错: {e}")

        return None

    def _safe_get_text(self, element):
        """安全地获取元素文本，处理编码问题"""
        try:
            text = element.get_text(strip=True)
            # 清理文本中的特殊字符
            text = re.sub(r'\s+', ' ', text)  # 替换多个空白字符为单个空格
            text = text.encode('utf-8', errors='ignore').decode('utf-8')  # 清理无法编码的字符
            return text
        except Exception:
            return ""

    def _has_next_page(self, soup):
        """检查是否有下一页"""
        # 查找下一页链接
        next_link = soup.find('a', string=re.compile(r'next', re.I)) or \
                    soup.find('a', class_=re.compile(r'next', re.I)) or \
                    soup.find('a', {'aria-label': re.compile(r'next', re.I)})

        return next_link is not None

    def download_pdf(self, pdf_url, save_path, filename=None):
        """下载单个PDF文件"""
        try:
            if not pdf_url:
                return False

            # 生成文件名
            if not filename:
                filename = os.path.basename(urlparse(pdf_url).path)
                if not filename.endswith('.pdf'):
                    filename = f"article_{hash(pdf_url)}.pdf"

            filename = self._sanitize_filename(filename)
            file_path = Path(save_path) / filename

            # 检查文件是否已存在
            if file_path.exists():
                logger.info(f"文件已存在，跳过: {filename}")
                return True

            logger.info(f"下载PDF: {pdf_url}")
            response = self.session.get(pdf_url, stream=True, timeout=60)
            response.raise_for_status()

            # 检查内容类型
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and not pdf_url.endswith('.pdf'):
                logger.warning(f"可能不是PDF文件: {content_type}")

            # 保存文件
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            file_size = file_path.stat().st_size
            logger.info(f"下载完成: {filename} ({file_size / 1024 / 1024:.2f} MB)")
            return True

        except Exception as e:
            logger.error(f"下载PDF失败 {pdf_url}: {e}")
            return False

    def save_articles_info(self, articles, output_dir):
        """保存文章信息到文件"""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # 保存为JSON
        json_file = output_dir / "articles_info.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(articles, f, indent=2, ensure_ascii=False)

        # 保存为CSV
        csv_file = output_dir / "articles_info.csv"
        if articles:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=articles[0].keys())
                writer.writeheader()
                writer.writerows(articles)

        logger.info(f"文章信息已保存到: {json_file} 和 {csv_file}")

    def download_all_pdfs(self, articles, save_path, max_concurrent=5):
        """批量下载所有PDF文件"""
        save_path = Path(save_path)
        save_path.mkdir(parents=True, exist_ok=True)

        success_count = 0
        failed_count = 0

        for i, article in enumerate(articles, 1):
            if not article.get('pdf_url'):
                logger.warning(f"[{i}/{len(articles)}] 没有PDF链接: {article.get('title', 'Unknown')}")
                failed_count += 1
                continue

            # 生成文件名
            title = article.get('title', 'unknown')
            filename = f"{i:04d}_{self._sanitize_filename(title)}.pdf"

            logger.info(f"[{i}/{len(articles)}] 处理: {title}")

            if self.download_pdf(article['pdf_url'], save_path, filename):
                success_count += 1
            else:
                failed_count += 1

            # 添加延迟
            time.sleep(1)

        logger.info(f"PDF下载完成! 成功: {success_count}, 失败: {failed_count}")
        return {'success': success_count, 'failed': failed_count}

    def _sanitize_filename(self, filename):
        """清理文件名"""
        # 移除非法字符
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'\s+', '_', filename)
        # 限制长度
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:200 - len(ext)] + ext
        return filename

    def crawl_and_download(self, search_url, output_dir="./cab_downloads", max_pages=None, download_pdfs=True):
        """完整的爬取和下载流程"""
        logger.info("开始爬取CAB Digital Library...")

        # 创建输出目录
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # 爬取文章信息
        articles = self.get_search_results(search_url, max_pages)

        if not articles:
            logger.error("没有找到任何文章")
            return

        # 保存文章信息
        self.save_articles_info(articles, output_dir)

        # 下载PDF文件
        if download_pdfs:
            pdf_dir = output_dir / "pdfs"
            self.download_all_pdfs(articles, pdf_dir)

        logger.info(f"爬取完成! 结果保存在: {output_dir}")


# 使用示例和测试功能
def test_access():
    """测试网站访问"""
    crawler = CABDigitalLibraryCrawler()

    # 测试访问主页
    try:
        logger.info("测试访问主页...")
        response = crawler.session.get(crawler.base_url, timeout=30)
        logger.info(f"主页访问状态: {response.status_code}")

        # 测试搜索页面
        search_url = "https://www.cabidigitallibrary.org/action/doSearch?SeriesKey=agrirxiv&startPage=0&sortBy=EPubDate"
        logger.info("测试访问搜索页面...")
        time.sleep(3)
        response = crawler.session.get(search_url, timeout=30)

        # 处理编码
        response.encoding = crawler._detect_encoding(response)

        logger.info(f"搜索页面访问状态: {response.status_code}")

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.title.string if soup.title else 'Unknown'
            # 安全地处理标题文本
            title = title.encode('utf-8', errors='ignore').decode('utf-8')
            logger.info(f"页面标题: {title}")
            return True
        else:
            logger.error(f"访问失败，状态码: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"访问测试失败: {e}")
        return False


def main():
    """主函数 - 包含多种运行模式"""
    # 模式1: 测试访问
    print("=== 模式1: 测试网站访问 ===")
    if not test_access():
        print("建议:")
        print("1. 检查网络连接")
        print("2. 使用代理服务器")
        print("3. 手动访问网站确认是否需要验证")
        return

    # 模式2: 少量测试爬取
    print("\n=== 模式2: 测试爬取(仅1页) ===")
    search_url = "https://www.cabidigitallibrary.org/action/doSearch?SeriesKey=agrirxiv&startPage=0&sortBy=EPubDate"

    crawler = CABDigitalLibraryCrawler()
    try:
        articles = crawler.get_search_results(search_url, max_pages=1)
        if articles:
            print(f"测试成功! 找到 {len(articles)} 篇文章")
            print("前3篇文章:")
            for i, article in enumerate(articles[:3], 1):
                print(f"{i}. {article.get('title', 'No title')}")
        else:
            print("没有找到文章，可能需要调整解析策略")
    except Exception as e:
        print(f"测试爬取失败: {e}")
        return

    # 模式3: 询问是否继续完整爬取
    print(f"\n=== 是否继续完整爬取? ===")
    choice = input("继续爬取所有页面并下载PDF? (y/n): ").lower()

    if choice == 'y':
        print("开始完整爬取...")
        crawler.crawl_and_download(
            search_url=search_url,
            output_dir="./agrirxiv_downloads",
            max_pages=None,  # 爬取所有页面
            download_pdfs=True
        )
    else:
        print("爬取已取消")


if __name__ == "__main__":
    main()