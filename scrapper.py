from socket_utils import get
from bs4 import BeautifulSoup


images_ext = ['jpg', 'jpeg', 'png', 'svg']


def get_data(http_resp, http_content, hostname, port_num):
    images_links = []
    _host_port_uri = []
    if http_resp.status == 200:
        soup = BeautifulSoup(http_content, 'lxml')
        imgs = soup.find_all('img')
        for img in imgs:
            try:
                src = img['src']
                if src[:2] == "//" and src.split(".")[-1] in images_ext:
                    images_links.append(f"https://{src[2:]}")
            except:
                continue
        anchors = soup.find_all('a')
        for anchor in anchors:
            try:
                href = anchor['href']
                if href[1:5] == "wiki":
                    _host_port_uri.append((hostname, port_num, href))
            except:
                continue
    return images_links, _host_port_uri


if __name__ == "__main__":
    host, port, uri = "en.wikipedia.org", 443, "/wiki/Main_Page"
    resp, content = get(host, port, uri)
    images, host_port_uri = get_data(resp, content, host, port)
    print(f"Scrapping {len(host_port_uri)} links for images")
    for _host, _port, _uri in host_port_uri:
        _resp, _content = get(_host, _port, _uri)
        _images, host_port_uri = get_data(_resp, _content, host, port)
        images.extend(_images)
    with open("images.txt", "w+") as fp:
        fp.write("\n".join(list(set(images))))

# _resp, _content = get("en.wikipedia.org", 443, "/wiki/Main_Page")
# print(get_data(_resp, _content, "en.wikipedia.org", 443))
