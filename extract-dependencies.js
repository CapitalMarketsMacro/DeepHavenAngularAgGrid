const fs = require('fs');
const path = require('path');

// Read package-lock.json
const lockFile = JSON.parse(fs.readFileSync('package-lock.json', 'utf8'));

// Extract all packages with their versions
const allDependencies = new Set();

// Function to recursively collect all dependencies
function collectDependencies(packages, parentPath = '') {
  for (const [packagePath, packageData] of Object.entries(packages)) {
    if (packagePath === '') continue; // Skip root package
    
    const packageName = packageData.name || packagePath.split('node_modules/').pop();
    const version = packageData.version;
    
    if (packageName && version) {
      allDependencies.add(`${packageName}@${version}`);
    }
    
    // Recursively process dependencies
    if (packageData.dependencies) {
      collectDependencies(packageData.dependencies, packagePath);
    }
  }
}

// Collect all dependencies from the packages object
if (lockFile.packages) {
  collectDependencies(lockFile.packages);
}

// Sort dependencies alphabetically
const sortedDependencies = Array.from(allDependencies).sort();

// Write to file
fs.writeFileSync('npm-all-dependencies.txt', sortedDependencies.join('\n'));

console.log(`Created npm-all-dependencies.txt with ${sortedDependencies.length} dependencies`);

// Also create a JSON version with more details
const detailedDeps = [];
for (const [packagePath, packageData] of Object.entries(lockFile.packages)) {
  if (packagePath === '') continue;
  
  const packageName = packageData.name || packagePath.split('node_modules/').pop();
  if (packageName && packageData.version) {
    detailedDeps.push({
      name: packageName,
      version: packageData.version,
      path: packagePath,
      license: packageData.license || 'N/A',
      dev: packageData.dev || false,
      optional: packageData.optional || false,
      resolved: packageData.resolved || 'N/A',
      integrity: packageData.integrity || 'N/A'
    });
  }
}

// Sort by name
detailedDeps.sort((a, b) => a.name.localeCompare(b.name));

fs.writeFileSync('npm-all-dependencies.json', JSON.stringify(detailedDeps, null, 2));

console.log(`Created npm-all-dependencies.json with ${detailedDeps.length} detailed dependency entries`);
