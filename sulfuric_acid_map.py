import pandas as pd
import re
import os
import webbrowser
import json

def parse_coordinates(coord_str):
    """Convert DMS coordinates to decimal degrees."""
    if not coord_str or not isinstance(coord_str, str):
        return None, None
    
    try:
        # Clean up the coordinate string
        cleaned_str = str(coord_str).strip()
        
        # Define regex patterns for different coordinate formats
        patterns = [
            # "37° 18' 3" N, 77° 16' 14" W"
            r'(\d+)[°º]\s*(\d+)[\'′]\s*(\d+(?:\.\d+)?)[\"″]?\s*([NSEW]),\s*(\d+)[°º]\s*(\d+)[\'′]\s*(\d+(?:\.\d+)?)[\"″]?\s*([NSEW])',
            # "40°50'55"N 84°04'51"W"
            r'(\d+)[°º]\s*(\d+)[\'′]\s*(\d+(?:\.\d+)?)[\"″]?\s*([NSEW])\s+(\d+)[°º]\s*(\d+)[\'′]\s*(\d+(?:\.\d+)?)[\"″]?\s*([NSEW])'
        ]
        
        # Try each pattern
        for pattern in patterns:
            match = re.search(pattern, cleaned_str, re.IGNORECASE)
            if match:
                # Extract lat/long components
                lat_deg = float(match.group(1))
                lat_min = float(match.group(2)) / 60
                lat_sec = float(match.group(3)) / 3600
                lat_dir = match.group(4).upper()
                
                lon_deg = float(match.group(5))
                lon_min = float(match.group(6)) / 60
                lon_sec = float(match.group(7)) / 3600
                lon_dir = match.group(8).upper()
                
                # Calculate decimal degrees
                latitude = lat_deg + lat_min + lat_sec
                if lat_dir == 'S':
                    latitude = -latitude
                    
                longitude = lon_deg + lon_min + lon_sec
                if lon_dir == 'W':
                    longitude = -longitude
                    
                return latitude, longitude
        
        # Special case for specific coordinates
        if "33° 46' 36\" N, 118° 17' 0\" W" in cleaned_str:
            return 33.77, -118.283333
        
        # Try simplified pattern without comma
        simplified_pattern = r'(\d+)[°º](\d+)[\'′](\d+)[\"″]?([NS])\s+(\d+)[°º](\d+)[\'′](\d+)[\"″]?([EW])'
        match = re.search(simplified_pattern, cleaned_str, re.IGNORECASE)
        if match:
            lat_deg = float(match.group(1))
            lat_min = float(match.group(2)) / 60
            lat_sec = float(match.group(3)) / 3600
            lat_dir = match.group(4).upper()
            
            lon_deg = float(match.group(5))
            lon_min = float(match.group(6)) / 60
            lon_sec = float(match.group(7)) / 3600
            lon_dir = match.group(8).upper()
            
            latitude = lat_deg + lat_min + lat_sec
            if lat_dir == 'S':
                latitude = -latitude
                
            longitude = lon_deg + lon_min + lon_sec
            if lon_dir == 'W':
                longitude = -longitude
                
            return latitude, longitude
            
        print(f"Could not parse coordinates: {coord_str}")
        return None, None
    except Exception as e:
        print(f"Error processing coordinates: {coord_str} - {e}")
        return None, None

def process_data_file(file_path):
    """Process the data file (CSV or Excel) and extract producer data with coordinates."""
    # Determine file type by extension
    file_extension = os.path.splitext(file_path)[1].lower()
    
    try:
        if file_extension == '.csv':
            print(f"Reading CSV file: {file_path}")
            df = pd.read_csv(file_path, encoding='utf-8')
        elif file_extension in ['.xlsx', '.xls']:
            print(f"Reading Excel file: {file_path}")
            # Try different sheet names or the first sheet
            try:
                df = pd.read_excel(file_path, sheet_name="North America Producers")
                print(f"Found sheet 'North America Producers'")
            except:
                try:
                    df = pd.read_excel(file_path, sheet_name="Sheet1")
                    print(f"Found sheet 'Sheet1'")
                except:
                    df = pd.read_excel(file_path)
                    print(f"Using first sheet as fallback")
        else:
            print(f"Unsupported file format: {file_extension}")
            return None
            
        print(f"Successfully read data file: {file_path}")
    except Exception as e:
        print(f"Error reading file: {e}")
        return None
    
    # Print column names to verify
    print("Columns in file:", df.columns.tolist())
    
    # Look for key columns with different possible names
    company_col = next((col for col in df.columns if col.lower() in ['owner', 'company', 'producer']), None)
    coords_col = next((col for col in df.columns if col.lower() in ['coordinates', 'coords', 'location']), None)
    planttype_col = next((col for col in df.columns if col.lower() in ['type of plant', 'plant type', 'planttype']), None)
    address_col = next((col for col in df.columns if col.lower() in ['address', 'addr']), None)
    city_col = next((col for col in df.columns if col.lower() in ['city', 'town']), None)
    state_col = next((col for col in df.columns if col.lower() in ['state', 'province']), None)
    country_col = next((col for col in df.columns if col.lower() in ['country', 'nation']), None)
    gassource_col = next((col for col in df.columns if col.lower() in ['gas source', 'gas_source', 'source']), None)
    capacity_col = next((col for col in df.columns if col.lower() in ['plant capacity', 'capacity']), None)
    
    if not company_col:
        print("Missing company column!")
        return None
    
    if not coords_col:
        print("Missing coordinates column!")
        return None
    
    # Process coordinates
    print("Processing coordinates...")
    latitudes = []
    longitudes = []
    
    for index, row in df.iterrows():
        if coords_col and pd.notna(row[coords_col]):
            lat, lon = parse_coordinates(row[coords_col])
            latitudes.append(lat)
            longitudes.append(lon)
        else:
            latitudes.append(None)
            longitudes.append(None)
    
    df['latitude'] = latitudes
    df['longitude'] = longitudes
    
    # Create a clean dataset for mapping
    mapping_data = []
    
    for index, row in df.iterrows():
        producer = {
            'company': str(row.get(company_col, 'Unknown')) if pd.notna(row.get(company_col)) else 'Unknown',
            'address': str(row.get(address_col, 'Not available')) if address_col and pd.notna(row.get(address_col)) else 'Not available',
            'city': str(row.get(city_col, 'Not available')) if city_col and pd.notna(row.get(city_col)) else 'Not available',
            'state': str(row.get(state_col, 'Not available')) if state_col and pd.notna(row.get(state_col)) else 'Not available',
            'country': str(row.get(country_col, 'Not available')) if country_col and pd.notna(row.get(country_col)) else 'Not available',
            'coordinates': str(row.get(coords_col, 'Not available')) if pd.notna(row.get(coords_col)) else 'Not available',
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'plantType': str(row.get(planttype_col, 'Not specified')) if planttype_col and pd.notna(row.get(planttype_col)) else 'Not specified',
            'gasSource': str(row.get(gassource_col, 'Not specified')) if gassource_col and pd.notna(row.get(gassource_col)) else 'Not specified',
            'capacity': str(row.get(capacity_col, 'Not specified')) if capacity_col and pd.notna(row.get(capacity_col)) else 'Not specified'
        }
        mapping_data.append(producer)
    
    valid_coords = sum(1 for item in mapping_data if item['latitude'] is not None and item['longitude'] is not None)
    print(f"Found {valid_coords} valid coordinates out of {len(mapping_data)} records")
    
    return mapping_data

def create_html_map(data, output_path="sulfuric_acid_map.html"):
    """Create an HTML file with the interactive map using the processed data."""
    html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sulfuric Acid Producers Map</title>
    <!-- Load the required libraries in the correct order -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">
    
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    
    <style>
        body {
            margin: 0;
            padding: 0;
            font-family: Arial, sans-serif;
        }
        #header {
            background-color: #2c3e50;
            color: white;
            padding: 15px;
            text-align: center;
        }
        #container {
            display: flex;
            height: calc(100vh - 130px);
        }
        #sidebar {
            width: 300px;
            padding: 15px;
            background-color: #f8f9fa;
            overflow-y: auto;
        }
        #map {
            flex-grow: 1;
            height: 100%;
        }
        .filter-section {
            margin-bottom: 15px;
            border-bottom: 1px solid #ddd;
            padding-bottom: 15px;
        }
        .filter-section h3 {
            margin-top: 0;
        }
        .legend {
            background-color: white;
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }
        .legend-item {
            margin: 5px 0;
        }
        .marker-icon {
            display: inline-block;
            width: 15px;
            height: 15px;
            border-radius: 50%;
            margin-right: 5px;
        }
        .marker-sulfur { background-color: #3388ff; }
        .marker-metallurgical { background-color: #dc3545; }
        .marker-regeneration { background-color: #28a745; }
        .marker-other { background-color: #6c757d; }
        .footer {
            background-color: #f8f9fa;
            text-align: center;
            padding: 10px;
            font-size: 12px;
            color: #6c757d;
        }
        .stat-box {
            background-color: #e9ecef;
            border-radius: 5px;
            padding: 10px;
            margin-bottom: 10px;
        }
        .stat-box h4 {
            margin: 0 0 5px 0;
        }
        .search-box {
            margin-bottom: 15px;
        }
        #search-input {
            width: 100%;
            padding: 8px;
            box-sizing: border-box;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .plant-list {
            max-height: 300px;
            overflow-y: auto;
            margin-top: 10px;
        }
        .plant-item {
            padding: 8px;
            border-bottom: 1px solid #eee;
            cursor: pointer;
        }
        .plant-item:hover {
            background-color: #f0f0f0;
        }
        @media (max-width: 768px) {
            #container {
                flex-direction: column;
                height: auto;
            }
            #sidebar {
                width: 100%;
                max-height: 300px;
            }
            #map {
                height: 400px;
            }
        }
    </style>
</head>
<body>
    <div id="header">
        <h1>Sulfuric Acid Producers Map</h1>
        <p>Interactive map of sulfuric acid production facilities across North America</p>
    </div>
    
    <div id="container">
        <div id="sidebar">
            <div class="search-box">
                <input type="text" id="search-input" placeholder="Search by company or location...">
            </div>
            
            <div class="filter-section">
                <h3>Filters</h3>
                <div>
                    <input type="checkbox" id="filter-sulfur" checked>
                    <label for="filter-sulfur">Sulfur Burner</label>
                </div>
                <div>
                    <input type="checkbox" id="filter-metallurgical" checked>
                    <label for="filter-metallurgical">Metallurgical</label>
                </div>
                <div>
                    <input type="checkbox" id="filter-regeneration" checked>
                    <label for="filter-regeneration">Acid Regeneration</label>
                </div>
                <div>
                    <input type="checkbox" id="filter-other" checked>
                    <label for="filter-other">Other Types</label>
                </div>
            </div>
            
            <div class="filter-section">
                <h3>Country</h3>
                <div>
                    <input type="checkbox" id="country-usa" checked>
                    <label for="country-usa">USA</label>
                </div>
                <div>
                    <input type="checkbox" id="country-canada" checked>
                    <label for="country-canada">Canada</label>
                </div>
                <div>
                    <input type="checkbox" id="country-mexico" checked>
                    <label for="country-mexico">Mexico</label>
                </div>
            </div>
            
            <div class="filter-section">
                <h3>Statistics</h3>
                <div class="stat-box">
                    <h4>Plant Count by Type</h4>
                    <div id="plant-type-stats"></div>
                </div>
                <div class="stat-box">
                    <h4>Plant Count by Country</h4>
                    <div id="country-stats"></div>
                </div>
            </div>
            
            <div class="filter-section">
                <h3>Producer List</h3>
                <div class="plant-list" id="plant-list"></div>
            </div>
        </div>
        
        <div id="map"></div>
    </div>
    
    <div class="footer">
        Created with Leaflet and MarkerCluster. Data extracted from Sulfuric Acid Producers data.
    </div>
    
    <script>
        // Producer data - loaded directly from data file
        const producerData = PRODUCER_DATA_PLACEHOLDER;
        
        // Initialize the map when the page is fully loaded
        document.addEventListener('DOMContentLoaded', function() {
            // Initialize the map centered on North America
            const map = L.map('map').setView([40, -100], 4);
            
            // Add the base tile layer (OpenStreetMap)
            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
                maxZoom: 19
            }).addTo(map);
            
            // Create a marker cluster group
            const markers = L.markerClusterGroup();
            
            // Define marker icons for different plant types
            const icons = {
                'sulfur': L.divIcon({
                    html: '<i class="fas fa-industry" style="color: #3388ff;"></i>',
                    className: 'custom-div-icon',
                    iconSize: [30, 30],
                    iconAnchor: [15, 15]
                }),
                'metallurgical': L.divIcon({
                    html: '<i class="fas fa-industry" style="color: #dc3545;"></i>',
                    className: 'custom-div-icon',
                    iconSize: [30, 30],
                    iconAnchor: [15, 15]
                }),
                'regeneration': L.divIcon({
                    html: '<i class="fas fa-industry" style="color: #28a745;"></i>',
                    className: 'custom-div-icon',
                    iconSize: [30, 30],
                    iconAnchor: [15, 15]
                }),
                'other': L.divIcon({
                    html: '<i class="fas fa-industry" style="color: #6c757d;"></i>',
                    className: 'custom-div-icon',
                    iconSize: [30, 30],
                    iconAnchor: [15, 15]
                })
            };
            
            // Create marker and add to the cluster group
            function createMarker(producer) {
                try {
                    if (!producer.latitude || !producer.longitude || isNaN(producer.latitude) || isNaN(producer.longitude)) return null;
                    
                    const plantType = producer.plantType.toLowerCase();
                    let iconType = 'other';
                    
                    if (plantType.includes('sulfur')) {
                        iconType = 'sulfur';
                    } else if (plantType.includes('metallurgical')) {
                        iconType = 'metallurgical';
                    } else if (plantType.includes('regeneration') || plantType.includes('regenerat')) {
                        iconType = 'regeneration';
                    }
                    
                    const popupContent = `
                        <div style="min-width: 200px; max-width: 300px;">
                            <h3>${producer.company}</h3>
                            <p><b>Location:</b> ${producer.city}, ${producer.state}</p>
                            <p><b>Country:</b> ${producer.country}</p>
                            <p><b>Plant Type:</b> ${producer.plantType}</p>
                            <p><b>Gas Source:</b> ${producer.gasSource}</p>
                            <p><b>Capacity:</b> ${producer.capacity}</p>
                        </div>
                    `;
                    
                    const marker = L.marker([producer.latitude, producer.longitude], {
                        icon: icons[iconType],
                        alt: producer.company,
                        plantType: iconType,
                        country: producer.country.toLowerCase()
                    }).bindPopup(popupContent);
                    
                    return marker;
                } catch (e) {
                    console.error("Error creating marker:", e, producer);
                    return null;
                }
            }
            
            // Add all markers to the map
            function addAllMarkers() {
                try {
                    // Clear existing markers
                    markers.clearLayers();
                    
                    // Stats counters
                    const stats = {
                        types: {
                            sulfur: 0,
                            metallurgical: 0,
                            regeneration: 0,
                            other: 0
                        },
                        countries: {
                            usa: 0,
                            canada: 0,
                            mexico: 0,
                            other: 0
                        }
                    };
                    
                    // Filter the data based on selections
                    const filteredData = producerData.filter(producer => {
                        try {
                            // Check if we have valid coordinates
                            if (!producer.latitude || !producer.longitude || isNaN(producer.latitude) || isNaN(producer.longitude)) return false;
                            
                            // Check plant type filters
                            const plantType = (producer.plantType || "").toLowerCase();
                            let typeMatch = false;
                            let typeCategory = 'other';
                            
                            if (plantType.includes('sulfur')) {
                                typeMatch = document.getElementById('filter-sulfur').checked;
                                typeCategory = 'sulfur';
                            } else if (plantType.includes('metallurgical')) {
                                typeMatch = document.getElementById('filter-metallurgical').checked;
                                typeCategory = 'metallurgical';
                            } else if (plantType.includes('regeneration') || plantType.includes('regenerat')) {
                                typeMatch = document.getElementById('filter-regeneration').checked;
                                typeCategory = 'regeneration';
                            } else {
                                typeMatch = document.getElementById('filter-other').checked;
                            }
                            
                            // Check country filters
                            const country = (producer.country || "").toLowerCase();
                            let countryMatch = false;
                            let countryCategory = 'other';
                            
                            if (country.includes('usa') || country.includes('united states')) {
                                countryMatch = document.getElementById('country-usa').checked;
                                countryCategory = 'usa';
                            } else if (country.includes('canada')) {
                                countryMatch = document.getElementById('country-canada').checked;
                                countryCategory = 'canada';
                            } else if (country.includes('mexico')) {
                                countryMatch = document.getElementById('country-mexico').checked;
                                countryCategory = 'mexico';
                            } else {
                                countryMatch = true; // Keep other countries visible by default
                            }
                            
                            // Check search filter
                            const searchText = document.getElementById('search-input').value.toLowerCase();
                            const searchMatch = searchText === '' || 
                                            (producer.company || "").toLowerCase().includes(searchText) ||
                                            (producer.city || "").toLowerCase().includes(searchText) ||
                                            (producer.state || "").toLowerCase().includes(searchText) ||
                                            (producer.country || "").toLowerCase().includes(searchText);
                            
                            // Update statistics if item passes all filters
                            if (typeMatch && countryMatch && searchMatch) {
                                stats.types[typeCategory]++;
                                stats.countries[countryCategory]++;
                            }
                            
                            return typeMatch && countryMatch && searchMatch;
                        } catch (e) {
                            console.error("Error filtering producer:", e, producer);
                            return false;
                        }
                    });
                    
                    console.log(`Adding ${filteredData.length} markers to the map`);
                    
                    // Create and add markers for filtered data
                    filteredData.forEach(producer => {
                        const marker = createMarker(producer);
                        if (marker) markers.addLayer(marker);
                    });
                    
                    map.addLayer(markers);
                    
                    // Update the statistics display
                    updateStats(stats);
                    
                    // Update the plant list
                    updatePlantList(filteredData);
                } catch (e) {
                    console.error("Error in addAllMarkers:", e);
                }
            }
            
            // Update statistics displays
            function updateStats(stats) {
                document.getElementById('plant-type-stats').innerHTML = `
                    <div>Sulfur Burner: ${stats.types.sulfur}</div>
                    <div>Metallurgical: ${stats.types.metallurgical}</div>
                    <div>Acid Regeneration: ${stats.types.regeneration}</div>
                    <div>Other: ${stats.types.other}</div>
                    <div><b>Total: ${stats.types.sulfur + stats.types.metallurgical + stats.types.regeneration + stats.types.other}</b></div>
                `;
                
                document.getElementById('country-stats').innerHTML = `
                    <div>USA: ${stats.countries.usa}</div>
                    <div>Canada: ${stats.countries.canada}</div>
                    <div>Mexico: ${stats.countries.mexico}</div>
                    <div>Other: ${stats.countries.other}</div>
                `;
            }
            
            // Update the plant list in sidebar
            function updatePlantList(filteredData) {
                const plantList = document.getElementById('plant-list');
                plantList.innerHTML = '';
                
                filteredData.forEach(producer => {
                    const item = document.createElement('div');
                    item.className = 'plant-item';
                    item.innerHTML = `<b>${producer.company}</b><br>${producer.city}, ${producer.state}`;
                    
                    item.addEventListener('click', () => {
                        if (producer.latitude && producer.longitude) {
                            map.setView([producer.latitude, producer.longitude], 10);
                            
                            // Find and open the marker's popup
                            markers.eachLayer(marker => {
                                const latlng = marker.getLatLng();
                                if (latlng.lat === producer.latitude && latlng.lng === producer.longitude) {
                                    marker.openPopup();
                                }
                            });
                        }
                    });
                    
                    plantList.appendChild(item);
                });
            }
            
            // Add legend to the map
            const legend = L.control({ position: 'bottomright' });
            legend.onAdd = function (map) {
                const div = L.DomUtil.create('div', 'legend');
                div.innerHTML = `
                    <h4>Plant Types</h4>
                    <div class="legend-item"><span class="marker-icon marker-sulfur"></span> Sulfur Burner</div>
                    <div class="legend-item"><span class="marker-icon marker-metallurgical"></span> Metallurgical</div>
                    <div class="legend-item"><span class="marker-icon marker-regeneration"></span> Acid Regeneration</div>
                    <div class="legend-item"><span class="marker-icon marker-other"></span> Other</div>
                `;
                return div;
            };
            legend.addTo(map);
            
            // Add event listeners for filters
            document.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
                checkbox.addEventListener('change', addAllMarkers);
            });
            
            document.getElementById('search-input').addEventListener('input', addAllMarkers);
            
            // Initialize with all markers
            addAllMarkers();
        });
    </script>
</body>
</html>
    """
    
    # Insert the data into the template
    data_json = json.dumps(data, indent=2)
    html_content = html_template.replace("PRODUCER_DATA_PLACEHOLDER", data_json)
    
    # Save the HTML file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"Map saved to: {output_path}")
    return output_path

def main(file_path, output_html="sulfuric_acid_map.html"):
    """Main function to process data file and create the interactive map."""
    print(f"Processing file: {file_path}")
    data = process_data_file(file_path)
    
    if data:
        html_path = create_html_map(data, output_html)
        print(f"Map created successfully: {html_path}")
        
        # Open the map in the default browser
        if os.path.exists(html_path):
            print(f"Opening map in browser...")
            webbrowser.open('file://' + os.path.abspath(html_path))
        
        return True
    else:
        print("Failed to process data file.")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Create an interactive map from data of sulfuric acid producers.')
    parser.add_argument('data_file', help='Path to the data file (CSV or Excel) containing producer data')
    parser.add_argument('--output', '-o', default='sulfuric_acid_map.html', help='Output HTML file path')
    
    args = parser.parse_args()
    
    main(args.data_file, args.output)