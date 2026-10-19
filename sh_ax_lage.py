import os
import sys
from datetime import datetime

def getCurPath():
    return os.path.dirname(os.path.realpath(__file__))


class AlkisNASXMLFile:
    def __init__(self, filePath, mode, version='', encoding='utf-8'):
        self.version = version
        self.file = open(filePath, mode=mode, encoding=encoding)
        self.closed = False
        self.mode = mode
        if self.mode == 'w':
            headFileName = 'alkis_nas_xml_head.xml'
            if self.version != '':
                headFileName += '_' + self.version
            headFile = open(os.path.join(getCurPath(), headFileName), mode='r', encoding='utf-8')
            self.file.write(headFile.read())
            headFile.close()

    def __del__(self):
        self.close()

    def close(self):
        if self.closed:
            return

        self.closed = True
        if self.mode == 'w':
            tailFileName = 'alkis_nas_xml_tail.xml'
            if self.version != '':
                tailFileName += '_' + self.version
            tailFile = open(os.path.join(getCurPath(), tailFileName), mode='r', encoding='utf-8')
            self.file.write(tailFile.read())
            tailFile.close()

        self.file.close()

    def write(self, s):
        return self.file.write(s)


#AX_GeoreferenzierteGebaeudeadresse


def toChar(val, digits):
    if val == '' or not val.isnumeric() or len(val) > digits:
        return '#' * digits

    return val


def getSchluesselgesamt(land, regierungsbezirk, kreis, gemeinde, lage):
    land = toChar(land, 2)
    if regierungsbezirk == '':
        regierungsbezirk = '0'
    kreis = toChar(kreis, 2)
    gemeinde = toChar(gemeinde, 3)

    #schluesselgesamt = '(Lagebezeichnung zu \'' + land + regierungsbezirk + kreis + gemeinde + lage + '\' fehlt)'
    schluesselgesamt = land + regierungsbezirk + kreis + gemeinde + lage
    return schluesselgesamt


def parseId(line):
    index0 = line.find('="')
    if index0 < 0:
        return ''

    index1 = line.find('">')
    if index1 < 0:
        return ''

    return line[index0 + 2:index1]


def parseValue(feature, line):
    index0 = line.find('<')
    if index0 < 0:
        return

    index1 = line.find('>')
    if index1 < 0:
        return

    index2 = line.find('</')
    if index2 < 0:
        return

    field = line[index0+1:index1]
    value = line[index1+1:index2]
    feature[field] = value


schluesselgesamtMap = {}
def writeFeature(feature, nasXMLFile):
    gmlId = feature['gid']
    land = feature['land']
    regierungsbezirk = feature['regierungsbezirk']
    kreis = feature['kreis']
    gemeinde = feature['gemeinde']
    lage = feature['lage']
    bezeichnung = feature['bezeichnung']
    schluesselgesamt = getSchluesselgesamt(land, regierungsbezirk, kreis, gemeinde, lage)
    if schluesselgesamt in schluesselgesamtMap:
        return
    schluesselgesamtMap[schluesselgesamt] = 1

    anlass = '000000'
    beginnt = datetime.now().isoformat()
    advstandardmodell = 'DLKM'

    content = '      <gml:featureMember>\n' \
              '        <adv:AX_LagebezeichnungKatalogeintrag gml:id="' + gmlId + '">\n' \
              '          <gml:identifier codeSpace="http://www.adv-online.de/">urn:adv:oid:' + gmlId + \
              '</gml:identifier>\n' \
              '          <adv:lebenszeitintervall>\n' \
              '            <adv:AA_Lebenszeitintervall>\n' \
              '              <adv:beginnt>' + beginnt + '</adv:beginnt>\n' \
              '            </adv:AA_Lebenszeitintervall>\n' \
              '          </adv:lebenszeitintervall>\n' \
              '          <adv:modellart>\n' \
              '            <adv:AA_Modellart>\n' \
              '              <adv:advStandardModell>' + advstandardmodell + '</adv:advStandardModell>\n' \
              '            </adv:AA_Modellart>\n' \
              '          </adv:modellart>\n' \
              '          <adv:modellart>\n' \
              '            <adv:AA_Modellart>\n' \
              '              <adv:sonstigesModell>NWDKOM</adv:sonstigesModell>\n' \
              '            </adv:AA_Modellart>\n' \
              '          </adv:modellart>\n' \
              '          <adv:anlass>' + anlass + '</adv:anlass>\n' \
              '          <adv:schluesselGesamt>' + schluesselgesamt + '</adv:schluesselGesamt>\n' \
              '          <adv:bezeichnung>' + bezeichnung + '</adv:bezeichnung>\n' \
              '          <adv:schluessel>\n' \
              '            <adv:AX_VerschluesselteLagebezeichnung>\n' \
              '              <adv:land>' + land + '</adv:land>\n' \
              '              <adv:regierungsbezirk>' + regierungsbezirk + '</adv:regierungsbezirk>\n' \
              '              <adv:kreis>' + kreis + '</adv:kreis>\n' \
              '              <adv:gemeinde>' + gemeinde + '</adv:gemeinde>\n' \
              '              <adv:lage>' + lage + '</adv:lage>\n' \
              '            </adv:AX_VerschluesselteLagebezeichnung>\n' \
              '          </adv:schluessel>\n' \
              '        </adv:AX_LagebezeichnungKatalogeintrag>\n' \
              '      </gml:featureMember>\n'

    nasXMLFile.write(content)


def processFile(filePath, nasXMLFile):
    file = open(filePath, mode='r', encoding='utf-8')
    feature = {}

    for line in file:
        line = line.strip()
        if line.startswith('<AX_GeoreferenzierteGebaeudeadresse '):
            feature = {}
            feature['gid'] = parseId(line)
        elif line == '</AX_GeoreferenzierteGebaeudeadresse>':
            feature['lage'] = feature['strassenschluessel']
            if 'strassenname' in feature:
                feature['bezeichnung'] = feature['strassenname']
                writeFeature(feature, nasXMLFile)
            feature = {}
        else:
            parseValue(feature, line)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        xml_path = sys.argv[1]

        nasXMLFile = AlkisNASXMLFile(os.path.join(xml_path, 'katalogobjekte_bda_ax_lage.xml'), 'w')
        for name in os.listdir(xml_path):
            if name.lower().endswith('.xml'):
                filePath = os.path.join(xml_path, name)
                processFile(filePath, nasXMLFile)
        nasXMLFile.close()

        print('SH_AX_LAGE: =======Finished=========')
    else:
        print('SH_AX_LAGE: =======Error: No XML path=========')