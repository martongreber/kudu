#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from bs4 import BeautifulSoup
import sys
import os

if len(sys.argv) != 2:
    print("Usage: python process_reports.py reports_folder")
    sys.exit(1)

reports_folder = sys.argv[1]

# Ensure the directory exists
if not os.path.isdir(reports_folder):
    print(f"Error: Directory {reports_folder} does not exist.")
    sys.exit(1)

# Process each HTML file in the directory
for filename in os.listdir(reports_folder):
    if filename.endswith(".html"):
        html_path = os.path.join(reports_folder, filename)
        css_filename = filename.replace(".html", ".css")
        css_path = os.path.join(reports_folder, css_filename)

        print(f"Processing: {filename}")

        # Read the HTML file
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract CSS from <style> tags
        css_content = []
        for style_tag in soup.find_all("style"):
            if style_tag.string:
                css_content.append(style_tag.string.strip())  # Extract full CSS
            style_tag.extract()  # Remove <style> tag from HTML

        # Extract inline styles and convert them to CSS classes
        inline_styles = {}
        class_counter = 0

        for tag in soup.find_all(style=True):
            style = tag["style"]
            if style not in inline_styles:
                class_name = "class_%d" % class_counter
                inline_styles[style] = class_name
                class_counter += 1
            else:
                class_name = inline_styles[style]

            # Assign class and remove inline style
            tag["class"] = tag.get("class", []) + [class_name]
            del tag["style"]

        # Convert absolute paths in <a href="..."> to relative filenames
        for a_tag in soup.find_all("a", href=True):
            a_tag["href"] = os.path.basename(a_tag["href"])  # Extract only the file name

        # Write extracted CSS to the corresponding CSS file
        with open(css_path, "w", encoding="utf-8") as f:
            for css_block in css_content:
                f.write(css_block + "\n\n")  # Ensure proper formatting
            for style, class_name in inline_styles.items():
                f.write(".%s { %s !important; }\n" % (class_name, style))  # Add !important

        # Add a <link> tag referencing the new CSS file
        link_tag = soup.new_tag("link", rel="stylesheet", type="text/css", href=css_filename)
        soup.head.insert(0, link_tag)

        # Write the updated HTML file
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(str(soup))

        print(f"Processed: {filename} -> {css_filename}")

print(f"All HTML files in {reports_folder} processed successfully!")