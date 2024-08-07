import os
import xml.etree.ElementTree as ET


def find_and_write_named_to_kml(kml_file_paths, named_value, output_kml_file_path):
    """
    Filters through multiple KML files to find a specific namecd element and writes them to a new KML file.

    :param kml_file_paths: List of paths to the KML files.
    :param named_value: The value of the named element to find.
    :param output_kml_file_path: Path to the output KML file.
    """
    # Define the namespace (Google Earth KML namespace)
    namespace = {'kml': 'http://earth.google.com/kml/2.2'}
    matching_placemarks = []

    for kml_file_path in kml_file_paths:
        if not os.path.exists(kml_file_path):
            print(f"File does not exist: {kml_file_path}")
            continue

        # Parse the KML file
        tree = ET.parse(kml_file_path)
        root = tree.getroot()

        # Find all Placemark elements with the desired namecd value
        for placemark in root.findall('.//kml:Placemark', namespace):
            name_element = placemark.find('kml:name', namespace)
            if name_element is not None and name_element.text == named_value:
                matching_placemarks.append(placemark)

    if not matching_placemarks:
        print(f"No elements found with named value: {named_value}")
        return

    # Create a new KML root element
    new_kml = ET.Element('kml', xmlns='http://earth.google.com/kml/2.2')
    new_document = ET.SubElement(new_kml, 'Document')

    # Append matching Placemarks to the new KML document
    for placemark in matching_placemarks:
        new_document.append(placemark)

    # Write the new KML to the output file
    new_tree = ET.ElementTree(new_kml)
    new_tree.write(output_kml_file_path, encoding='utf-8', xml_declaration=True)
    print(f"New KML file created at: {output_kml_file_path}")


kml_file_paths = ["H:Shared drives//Stomato//2024//TStar Map KMLs//south 6-5-2024.kml",
                  "H:Shared drives//Stomato//2024//TStar Map KMLs//north 6-5-2024.kml",
                  "H:Shared drives//Stomato//2024//TStar Map KMLs//organics 6-5-2024.kml"]
named_value = '1263'
# 1779 1263
output_kml_file_path = f'H://Shared drives//Stomato//2024//KMLS//{named_value}.kml'

find_and_write_named_to_kml(kml_file_paths, named_value, output_kml_file_path)
