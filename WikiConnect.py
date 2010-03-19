#!/usr/bin/env python

import MySQLdb
import sys
import string
import time
import pycurl
import urllib
import StringIO
import re
import md5
import os
import filecmp

UsingRewriteRules = False

headerKeyValueRex = re.compile("^([^:]+): ([^\r]*)\r*$")
sourceTextAreaRex = re.compile(".*<textarea[^>]*>(.*)</textarea>",
  re.DOTALL | re.MULTILINE)
editTokenRex = re.compile(".*value=\"([^>]*)\"[^>]*name=.wpEditToken.*",
  re.DOTALL)
editTimeRex = re.compile(".*value=\"([^>]*)\"[^>]*name=.wpEdittime.*",
  re.DOTALL)
confirmUploadToken = re.compile("<[^>]*wpSessionKey[^>]*\"([0-9][0-9]*)\" */>",
  re.DOTALL | re.MULTILINE);

def CurlDebug(t, b):
  print "<b>CurlDebug(%d): %s</b><br />" % (t, b)

#def CleanString(str):
#  res = ""
#  for a in UnEncodeString(str):
#    if a == '_': res += "\\_"
#    elif a == '%': res += "\\%"
#    elif a == '{': res += "\\{"
#    elif a == '}': res += "\\}"
#    elif a == '[': res += "\\["
#    elif a == ']': res += "\\]"
#    elif a == '\\': res += "\\\\"
#    elif a == '\r': pass
#    else: res += a
#  return res 

def UnEncodeString(str):
  res = string.replace(str, "&quot;", "\"")
  res = string.replace(res, "&lt;", "<")
  res = string.replace(res, "&gt;", ">")
  res = string.replace(res, "&amp;", "&")
  return res

def ConvertLinkPrefixes(content, oldPrefix, newPrefix):
  return string.replace(content, oldPrefix, newPrefix)

NEW = 0
CHANGED = 1
SAME = 2
NOIMAGE = 3
BADNAME = 4

class WikiObject:
  def __init__(self, wc, name):
    self.Name = name
    self.Status = NEW
    self.Connector = wc
  def __repr__(self):
    return "Object(%s)" % self.Name

class WikiImage(WikiObject):
  def __init__(self, wc, name):
    WikiObject.__init__(self, wc, name)
    self.Hash = wc.wfGetHashPath(self.Name)
  def GetStatus(self):
    return self.Connector.GetImageStatus(self)
  def SubmitToWikiWeb(self):
    return self.Connector.SubmitWikiWebImage(self)
  def __repr__(self):
    return "Image(%s)" % self.Name
  def __cmp__(self, other):
    return self.Name.__cmp__(other.Name)

class WikiPage(WikiObject):
  def __init__(self, wc, name):
    WikiObject.__init__(self, wc, name)
    self.WikiPageName = "%s%s" % (self.Connector.WikiServer.PAGE_PREFIX, name)
    self.WikiWebPageName = "%s%s" % (self.Connector.WikiWebServer.PAGE_PREFIX, name)
    #self.WikiWebPageName = "%sWeb:%s" % (self.Connector.WikiWebServer.PAGE_PREFIX, name)
    self.WikiPageSource = 0
    self.WikiWebPageSource = 0
  def GetWikiPageSource(self):
    if not self.WikiPageSource:
      self.WikiPageSource = self.Connector.GetWikiPageSource(self.WikiPageName)
    return self.WikiPageSource
  def GetWikiWebPageSource(self):
    if not self.WikiWebPageSource:
      self.WikiWebPageSource = self.Connector.GetWikiWebPageSource(self.WikiWebPageName)
    return self.WikiWebPageSource
  def GetStatus(self):
    if not self.Connector.ValidName(self.Name):
      return BADNAME
    source = self.GetWikiPageSource()
    destination = self.GetWikiWebPageSource()
    if not destination:
      return NEW
    sourcePrefix = self.Connector.WikiServer.PAGE_PREFIX
    destPrefix = self.Connector.WikiWebServer.PAGE_PREFIX
    source = ConvertLinkPrefixes(source, sourcePrefix, destPrefix)
    if source != destination:
      return CHANGED
    return SAME
  def SubmitToWikiWeb(self):
    content = self.GetWikiPageSource()
    sourcePrefix = self.Connector.WikiServer.PAGE_PREFIX
    destPrefix = self.Connector.WikiWebServer.PAGE_PREFIX
    content = ConvertLinkPrefixes(content, sourcePrefix, destPrefix)
    return self.Connector.SubmitWikiWebPage(self.WikiWebPageName, content)
  def GetPageImages(self):
    return self.Connector.GetPageImages(self.WikiPageName)

class WikiConnect:
  def __init__(self, wikiServer, wikiWebServer, cookiesFile):
    self.SQL = 0
    self.WikiLoggedIn = 0
    self.WikiWebLoggedIn = 0
    self.WikiServer = wikiServer
    self.WikiWebServer = wikiWebServer
    self.CookiesFile = cookiesFile
    self.ValidNameRex = re.compile("^[A-Za-z0-9_.:/-]*$")
   
  def ValidName(self, name):
    if not self.ValidNameRex.match(name):
      return False
    return True
  def MakeQuery(self, query, server):
    #print `query`
    sql = MySQLdb.connect(db=server.DATABASE, host=server.HOST,
      user=server.USER, passwd=server.PASSWORD)
    cursor = sql.cursor()
    cursor.execute(query)
    return cursor

  def GetListOfPages(self):
    if self.WikiServer.IS_NAMESPACE:
      cursor = self.MakeQuery("SELECT page_title FROM page WHERE page_namespace='%d'" % self.WikiServer.NAMESPACE, self.WikiServer) 
      pagesList = []
      while 1:
        res = cursor.fetchone()
        if not res:
          break
        pagesList.append(res[0])
    else:
      cursor = self.MakeQuery("SELECT page_title FROM page;", self.WikiServer)
      pagesList = []
      while 1:
        res = cursor.fetchone()
        if not res:
          break
        pageTitle = res[0]
        pageNameRex = re.compile("^"+self.WikiServer.PAGE_PREFIX+"(.*)")
        rex = pageNameRex.match(pageTitle)
        if not rex:
          continue
        pagesList.append(rex.group(1))
    return pagesList

  def GetPageImages(self, page_title):
    page_title = MySQLdb.escape_string(page_title)
    if self.WikiServer.IS_NAMESPACE:
      title = page_title[len(self.WikiServer.PAGE_PREFIX):]
      cursor = self.MakeQuery("SELECT imagelinks.il_to FROM page, imagelinks WHERE imagelinks.il_from=page.page_id AND page_title = '%s' AND page_namespace=%d" % (title, self.WikiServer.NAMESPACE), self.WikiServer)
    else:
      cursor = self.MakeQuery("SELECT imagelinks.il_to FROM page, imagelinks WHERE imagelinks.il_from=page.page_id AND page_title = '%s';" % page_title, self.WikiServer)
    images = []
    while 1:
      res = cursor.fetchone();
      if not res:
        break
      imageTitle = res[0]
      wi = WikiImage(self,imageTitle)
      images.append(wi)
    return images

  def wfGetHashPath (self, key):
    hash = md5.new(key).hexdigest()
    return '/%s/%s/' % (hash[0], hash[0:2])

  def GetImageStatus(self, image):
    if not self.ValidName(image.Name):
      return BADNAME
    propWiki = self.GetImageProperties(image.Name, self.WikiServer)
    wikiFileName = os.path.join("%s/images%s" % (self.WikiServer.PATH, image.Hash), image.Name)
    if not os.path.exists(wikiFileName):
    #  print "<pre>Cannot find image: [%s]</pre>" % wikiFileName
      return NOIMAGE
    #else:
    #  print "<pre>Found image: [%s]</pre>" % wikiFileName
    propWikiWeb = self.GetImageProperties(image.Name, self.WikiWebServer)
    if not propWikiWeb:
      return NEW
    wikiWebFileName = os.path.join("%s/images%s" % (self.WikiWebServer.PATH, image.Hash), image.Name)
    #print `propWiki`, `propWikiWeb`, `wikiFileName`, `wikiWebFileName`,"<br />"

    if filecmp.cmp(wikiFileName, wikiWebFileName, 0):
      return SAME
    return CHANGED

  def GetImageProperties(self, image_title, server):
    image_title = MySQLdb.escape_string(image_title)
    cursor = self.MakeQuery("SELECT * FROM image WHERE img_name = '%s';" % image_title,
      server)
    while 1:
      res = cursor.fetchone()
      if not res:
        break
      return res
    return ()

  def GetPageProperties(self, page_title, server, namespace):
    cursor = self.MakeQuery("SELECT * FROM page WHERE page_title = '%s' AND page_namespace=%d" % (page_title, namespace), server)
    while 1:
      res = cursor.fetchone()
      if not res:
        break
      return res
    return ()
  def GetNewPageSource(self, page_title, server):
    namespace = 0
    if page_title.startswith("Image:"):
      page_title = page_title[6:]
      namespace = 6
    elif page_title.startswith("MediaWiki:"):
      page_title = page_title[10:]
      namespace = 8 
    elif page_title.startswith(server.PAGE_PREFIX):
      if server.IS_NAMESPACE:
        page_title = page_title[len(server.PAGE_PREFIX):]
        namespace = server.NAMESPACE 
    prop = self.GetPageProperties(page_title, server, namespace)
    if not prop:
      return ""
    pageId = prop[0]
    rev = prop[9]
    #print "<!-- pageId: %d rev: %d -->" % (pageId, rev)
    cursor = self.MakeQuery("SELECT rev_text_id FROM revision WHERE rev_id = '%d' and rev_page = '%d';" %
      (rev, pageId), server)
    rev_text_id = 0
    while 1:
      res = cursor.fetchone()
      if not res:
        break
      rev_text_id = res[0]
      break
    if rev_text_id == 0:
      return ""
    #print "<!-- old_id: %d -->" % (rev_text_id)
    cursor = self.MakeQuery("SELECT old_text FROM text WHERE old_id = '%d';" %
      rev_text_id, server)
    while 1:
      res = cursor.fetchone()
      if not res:
        break
      page_src = res[0]
      if type(page_src) != type("andy"):
        #print "<!-- Funky type: %s <> %s <> %s -->" % (`type(page_src)`, `type("andy")`, `type([1, 2])`)
        page_src = page_src.tostring()
      return page_src
    return ""


  def GetWikiPageSource(self, page):
    return UnEncodeString(self.GetNewPageSource(page, self.WikiServer))

  def GetWikiWebPageSource(self, page):
    return UnEncodeString(self.GetNewPageSource(page, self.WikiWebServer))

  def WikiLogin(self, url, username, password):
    page = self.GetWikiPage(
      "%s?title=Special:Userlogin&amp;action=submitlogin" % url,
      [ ("wpName", username), ("wpPassword", password), 
        ("wpLoginattempt", "Log in") ])
    headers = page[0]
    #print page
    if headers.has_key("Location"):
      page = self.GetWikiPage(headers["Location"])
      #print page
    #print "------------------- logged in ----------------------"
    return 1

  def SubmitWikiWebPage(self, page, content):
    if UsingRewriteRules:
      WikiWebUrl = self.WikiWebServer.URL + "/index.php"
    else:
      WikiWebUrl = self.WikiWebServer.URL
    if not self.WikiWebLoggedIn:
      if not self.WikiLogin(WikiWebUrl,
        self.WikiWebServer.WEB_USER,
        self.WikiWebServer.WEB_PASSWORD):
        return 0
      self.WikiWebLoggedIn = 1
    pageContent = self.GetWikiPage(
      "%s?title=%s&action=edit" % (WikiWebUrl, page))
    rex = editTokenRex.match(pageContent[1])
    if not rex:
      sys.stderr.write("Cannot find token in page: %s\n" % page)
      print "No token[%s]" % pageContent[1]
      return 0
    token = rex.group(1)
    editTimeMatch = editTimeRex.match(pageContent[1])
    if not editTimeMatch:
      sys.stderr.write("Cannot find edit time token in page: %s\n" % page)
      print "No edit time[%s]" % pageContent[1]
      return 0
    editTime = editTimeMatch.group(1)
    url = "%s?title=%s&action=submit" % ( WikiWebUrl, page )
    form_data = [ ("wpTextbox1", content),
      ("wpSummary", "Update from Wiki"),
      ("wpMinoredit", ""),
      ("wpWatchthis", ""),
      ("wpSave", "Save page"),
      ("wpSection", ""),
      ("wpEdittime", editTime),
      ("wpEditToken", token) ]
    pageContent = self.GetWikiPage(url, form_data)
#    self.WritePage("foo.html", pageContent[1])
     
    src = self.GetWikiWebPageSource(page)
    
    if src != content:
      sys.stderr.write("Problem updating the page: %s\n" % page)
      print "Problem updating [%s] [%s]" % (src, content)
      f = file("src.txt", "w")
      f.write(src)
      f.close()
      f = file("cont.txt", "w")
      f.write(content)
      f.close()
      return 0
    return 1
  def SubmitWikiWebImage(self, image):
    if UsingRewriteRules:
      WikiWebUrl = self.WikiWebServer.URL + "/index.php"
    else:
      WikiWebUrl = self.WikiWebServer.URL
    if not self.WikiWebLoggedIn:
      if not self.WikiLogin(WikiWebUrl,
        self.WikiWebServer.WEB_USER,
        self.WikiWebServer.WEB_PASSWORD):
        return 0
      self.WikiWebLoggedIn = 1
    url = "%s/Special:Upload" % ( self.WikiWebServer.URL )
    wikiFileName = os.path.join("%s/images%s" % (self.WikiServer.PATH, image.Hash), image.Name)
    form_data = [ ("wpUploadFile", (pycurl.FORM_FILE, wikiFileName)),
      ("wpDestFile", image.Name),
      ("wpUploadDescription", ""),
      ("wpWatchthis", ""),
      ("wpUpload", "Upload File") ]
    #print "<pre>Upload URL: [%s]</pre>" % url
    pageContent = self.GetWikiPage(url, form_data)
    wikiWebFileName = os.path.join("%s/images%s" % (self.WikiWebServer.PATH, image.Hash), image.Name)
    if os.path.exists(wikiWebFileName) and filecmp.cmp(wikiFileName, wikiWebFileName, 0):
      # Upload successful
      return 1
    # find token
    rex = confirmUploadToken.search(pageContent[1])
    if not rex:
      sys.stderr.write("Cannot find token in page: %s\n" % image.Name)
      print "No token[%s]" % pageContent[1]
      return 0
    token = rex.group(1)
    url = "%s?title=Special:Upload&action=submit" % ( WikiWebUrl )
    #get the image description
    imgSrc = self.GetWikiPageSource("Image:" + image.Name)
    form_data = [
      ("wpIgnoreWarning", "1"),
      ("wpSessionKey", token),
      ("wpDestFile", image.Name),
      ("wpUploadDescription", imgSrc),
      ("wpWatchthis", ""),
      ("wpUpload", "Save File") ]
    pageContent = self.GetWikiPage(url, form_data)
    fp = open("/tmp/uploadImage.txt", "w")
    fp.write(pageContent[1])
    if not os.path.exists(wikiWebFileName) or not filecmp.cmp(wikiFileName, wikiWebFileName, 0):
      sys.stderr.write("Problem updating the image: %s\n" % image.Name)
      print "[%s]" % `pageContent`
      print "Cannot find file: %s\n" % wikiWebFileName
      return 0
    return 1
    
  def GetWikiPageObject(self, pageName):
    return WikiPage(self, pageName)
  def GetWikiImageObject(self, imageName):
    return WikiImage(self, imageName)

  def GetWikiPage(self, page, formMap = [], verbose = 0):
    #print "------------------- Get Page: %s ----------------------" % page
    #print "URL: %s" % page
    bw = StringIO.StringIO()
    hw = StringIO.StringIO()
    c = pycurl.Curl()
    c.setopt(pycurl.URL, "%s" % page)
    c.setopt(pycurl.COOKIEJAR, self.CookiesFile)
    c.setopt(pycurl.COOKIEFILE, self.CookiesFile)
    if formMap:
      form_data = urllib.urlencode(formMap)
      #print form_data
      #c.setopt(pycurl.POST, 1)
      #c.setopt(pycurl.POSTFIELDS, form_data)
      c.setopt(pycurl.HTTPPOST, formMap)
    c.setopt(pycurl.FOLLOWLOCATION,1);
    #c.setopt(pycurl.HEADER,1); 
    c.setopt(pycurl.WRITEFUNCTION, bw.write)
    c.setopt(pycurl.HEADERFUNCTION, hw.write)
    if verbose:
      c.setopt(pycurl.VERBOSE, 1)
      c.setopt(pycurl.DEBUGFUNCTION, CurlDebug)
    c.perform()
    c.close()
    kvpHeaders = {}
    for a in hw.getvalue().split("\n"):
      rex = headerKeyValueRex.match(a)
      if rex:
        kvpHeaders[rex.group(1)] = rex.group(2)
    if verbose:
      print "------------------- Before Page: %s ----------------------" % page
      print bw.getvalue()
      print "------------------- Done Page: %s ----------------------" % page
    return (kvpHeaders, bw.getvalue())
  def WritePage(self, file, page):
    fp = open(file, "w")
    fp.write(page)
    fp.close()

if __name__ == '__main__':
  wc = WikiConnect()
  
  update = 0
  pagesList = []
  if len(sys.argv) > 1:
    pagesList.append(sys.argv[1])
    if len(sys.argv) > 2:
      update = 1
  
  if not pagesList:
    pagesList = wc.GetListOfPages()
  print pagesList
  
  for page in pagesList:
    if wc.wikiServer.IS_NAMESPACE:
#      srcpage = wc.wikiServer.PAGE_PREFIX + ":" + page
      srcpage = wc.wikiServer.PAGE_PREFIX + page
    else:  
      srcpage = wc.wikiServer.PAGE_PREFIX + page
    sys.stderr.write("Read wiki: %s\n" % srcpage)
    source = wc.GetWikiPageSource(srcpage)
    wc.WritePage("srcpage_%s.txt" % page, source)
    sys.stderr.write("Read wikiWeb: %s\n" % page)
    destination = wc.GetWikiWebPageSource(page)
    wc.WritePage("dstpage_%s.txt" % page, destination)
    if source != destination:
      sys.stderr.write("---- Different: %s\n" % page)
      if update:
        if not wc.SubmitWikiWebPage(page, source):
          sys.stderr.write("Problem submitting page: %s\n" % page)
          sys.exit(1)
          
  

