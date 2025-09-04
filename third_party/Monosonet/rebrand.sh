#!/bin/bash

# Case-sensitive replacements
find . -type f \( -name "*.java" -o -name "*.md" -o -name "*.yml" -o -name "*.yaml" -o -name "*.xml" -o -name "*.properties" -o -name "*.gradle" -o -name "*.json" \) \
  -exec sed -i 's/OpenSearch/Sigma/g' {} +

  find . -type f \( -name "*.java" -o -name "*.md" -o -name "*.yml" -o -name "*.yaml" -o -name "*.xml" -o -name "*.properties" -o -name "*.gradle" -o -name "*.json" \) \
    -exec sed -i 's/opensearch/sigma/g' {} +

    find . -type f \( -name "*.java" -o -name "*.md" -o -name "*.yml" -o -name "*.yaml" -o -name "*.xml" -o -name "*.properties" -o -name "*.gradle" -o -name "*.json" \) \
      -exec sed -i 's/OPENSEARCH/SIGMA/g' {} +

      # Package names and imports
      find . -type f -name "*.java" \
        -exec sed -i 's/org\.opensearch/org.sigma/g' {} +

        # Directory and file renames
        find . -name "*opensearch*" -type d | while read dir; do
          mv "$dir" "${dir//opensearch/sigma}"
          done

          find . -name "*opensearch*" -type f | while read file; do
            mv "$file" "${file//opensearch/sigma}"
            done

            # Update main class references
            find . -type f -name "*.java" \
              -exec sed -i 's/OpenSearchMain/SigmaMain/g' {} +

              # Configuration file updates
              find . -name "*.yml" -o -name "*.yaml" \
                -exec sed -i 's/cluster\.name: opensearch/cluster.name: sigma/g' {} +

                echo "Rebranding complete - OpenSearch is now Sigma"