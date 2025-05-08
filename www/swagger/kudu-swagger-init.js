function initializeSwaggerUI() {
  console.log('Initializing Swagger UI...');
  console.log('SwaggerUIBundle available:', typeof SwaggerUIBundle !== 'undefined');

  if (typeof SwaggerUIBundle === 'undefined') {
    document.getElementById('swagger-ui').innerHTML = '<div style="color: red; padding: 20px; border: 1px solid red;">ERROR: SwaggerUIBundle is not loaded! Check browser console for details.</div>';
    return;
  }

  try {
    const swaggerContainer = document.getElementById('swagger-ui');
    const baseUrl = swaggerContainer ? swaggerContainer.getAttribute('data-base-url') || '' : '';
    console.log('Base URL:', baseUrl);

      let specUrl;

      if (baseUrl && baseUrl.includes('?')) {
        const [basePath, queryParams] = baseUrl.split('?', 2);
        // Remove trailing slashes to prevent double slashes
        const cleanBasePath = basePath.replace(/\/+$/, '');
        specUrl = `${cleanBasePath}/api/v1/spec?${queryParams}`;
      }
      else {
        const cleanBasePath = baseUrl.replace(/\/+$/, '');
        specUrl = `${cleanBasePath}/api/v1/spec`;
      }
      console.log('API spec URL:', specUrl);

      const ui = SwaggerUIBundle({
        url: specUrl,
      dom_id: '#swagger-ui',
      deepLinking: true,
      presets: [
        SwaggerUIBundle.presets.apis
      ],
      plugins: [
        SwaggerUIBundle.plugins.DownloadUrl
      ],
      layout: "BaseLayout"
    });

    console.log('Swagger UI initialized successfully');
  } catch (error) {
    console.error('Error initializing Swagger UI:', error);
    document.getElementById('swagger-ui').innerHTML = '<div style="color: red; padding: 20px; border: 1px solid red;">ERROR: ' + error.message + '</div>';
  }
}

// Initialize when DOM is ready
window.addEventListener('load', initializeSwaggerUI);