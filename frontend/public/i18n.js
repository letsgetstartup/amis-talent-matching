// i18n.js - Internationalization dictionary for English and Spanish
const translations = {
  en: {
    jobs: {
      title: "Jobs",
      searchPlaceholder: "Search by title/city/text",
      refreshButton: "Refresh",
      tableHeaders: {
        id: "ID",
        external: "External",
        title: "Title",
        city: "City",
        ai: "AI",
        updated: "Updated",
        actions: "Actions"
      }
    },
    candidates: {
      title: "Candidates",
      searchPlaceholder: "Search by name/role/city/skill",
      refreshButton: "Refresh",
      tableHeaders: {
        id: "ID",
        share: "Share",
        name: "Name",
        title: "Title",
        city: "City",
        updated: "Updated",
        matches: "Matches",
        letter: "Letter",
        actions: "Actions"
      },
      allFieldsTitle: "All Fields (Wide View)"
    },
    csvImports: {
      jobUploadTitle: "Job Upload",
      importFileTitle: "Import File (CSV/XLSX/XLSM)",
      importFileButton: "Import File",
      importHint: "Supports Hebrew/English headers. Adds &format=agency_template",
      processingText: "Processing with AI...",
      singleJobTitle: "Single Job",
      placeholders: {
        externalJobId: "External Job ID",
        title: "Title",
        city: "City",
        contactEmail: "Contact Email",
        description: "Description",
        required: "Required: separated by commas/lines",
        nice: "Nice: separated by commas/lines"
      },
      createButton: "Create",
      candidateUploadTitle: "Candidate Upload",
      multipleFilesTitle: "Multiple Files (PDF/TXT/DOCX)",
      uploadHint: "Support for PDF/DOCX/TXT and also CSV in format: Candidate Number, Candidate, Order Number, Education, Experience, Phone, Email, City",
      uploadButton: "Upload"
    },
    matchesChat: {
      title: "Candidate Matches to Jobs",
      scoreCalculationTitle: "How is the score calculated?",
      showHideButton: "Show/Hide",
      searchMatchesTitle: "Search Matches by ID",
      searchTypeLabel: "Search Type",
      candidateRadio: "Candidate",
      jobRadio: "Job",
      idLabel: "ID (24 hex digits)",
      exampleId: "Example: 68af2e02697377761f45e4a6",
      topKLabel: "Top K",
      filterByCityLabel: "Filter by City",
      geographicProximity: "Geographic Proximity",
      searchMatchesButton: "Search Matches",
      matchingControlsTitle: "Control Matching Parameters (Real-time Refresh)",
      proximityFilterLabel: "Filter by Proximity",
      cacheStrategyLabel: "Cache Strategy",
      maxAgeLabel: "Max Age (seconds)",
      exampleAge: "Example: 86400",
      requiredProfessionEscoLabel: "Required Profession ESCO",
      exampleRpEsco: "Example: 3115",
      fieldOfOccupationEscoLabel: "Field of Occupation ESCO",
      exampleFoEsco: "Example: 7223",
      weightingTitle: "Score Weighting and Settings (Server)",
      mainWeightsTitle: "Main Weights (0-1)",
      weights: {
        skills: "Skills Weight",
        title: "Title Weight",
        semantic: "Semantic Weight",
        embedding: "Embedding Weight",
        distance: "Distance Weight"
      },
      exampleWeights: "Example: 0.30",
      categoriesTitle: "Categories and Caps",
      categoryWeights: {
        required: "Required Category Weight",
        nice: "Nice Category Weight",
        skillsFloor: "Skills Floor (Minimum)"
      },
      sumHint: "Recommended sum required+nice = 1",
      exampleCw: "Example: 0.70",
      buttons: {
        loadFromServer: "Load from Server",
        normalizeWeights: "Normalize Weights",
        applyLocally: "Apply Locally",
        saveToServer: "Save to Server"
      },
      refreshButton: "Refresh",
      clearFilterButton: "Clear Filter",
      undoButton: "Undo",
      redoButton: "Redo",
      tableHeaders: {
        candidate: "Candidate",
        role: "Role",
        numberOfMatches: "Number of Matches",
        top3: "Top 3"
      },
      chatTitle: "Matches Chat",
      chatPlaceholder: "Example: How many matches found today?",
      sendButton: "Send",
      chatHint: "The chat uses data from Mongo and OpenAI API via FastAPI"
    },
    copilot: {
      hint: "Assistant is connected to your workspace. Ask anything.",
      placeholder: "Message Copilot...",
      sendButton: "Send",
      hint2: "Answers stream live; Copilot can use system data and actions."
    },
    common: {
      save: "Save",
      cancel: "Cancel",
      loading: "Loading...",
      error: "Error",
      success: "Success",
      notConnected: "Not connected"
    },
    tabs: {
      dashboard: "Dashboard",
      jobs: "Jobs",
      candidates: "Candidates",
      matches: "Matches+Chat",
      copilot: "Copilot",
      upload: "Upload",
      imports: "CSV Import",
      logout: "Logout"
    }
  },
  es: {
    jobs: {
      title: "Trabajos",
      searchPlaceholder: "Buscar por título/ciudad/texto",
      refreshButton: "Actualizar",
      tableHeaders: {
        id: "ID",
        external: "Externo",
        title: "Título",
        city: "Ciudad",
        ai: "IA",
        updated: "Actualizado",
        actions: "Acciones"
      }
    },
    candidates: {
      title: "Candidatos",
      searchPlaceholder: "Buscar por nombre/rol/ciudad/habilidad",
      refreshButton: "Actualizar",
      tableHeaders: {
        id: "ID",
        share: "Compartir",
        name: "Nombre",
        title: "Título",
        city: "Ciudad",
        updated: "Actualizado",
        matches: "Coincidencias",
        letter: "Carta",
        actions: "Acciones"
      },
      allFieldsTitle: "Todos los Campos (Vista Amplia)"
    },
    csvImports: {
      jobUploadTitle: "Carga de Trabajos",
      importFileTitle: "Importar Archivo (CSV/XLSX/XLSM)",
      importFileButton: "Importar Archivo",
      importHint: "Soporta encabezados en hebreo/inglés. Agrega &format=agency_template",
      processingText: "Procesando con IA...",
      singleJobTitle: "Trabajo Individual",
      placeholders: {
        externalJobId: "ID de Trabajo Externo",
        title: "Título",
        city: "Ciudad",
        contactEmail: "Correo de Contacto",
        description: "Descripción",
        required: "Requerido: separado por comas/líneas",
        nice: "Bonito: separado por comas/líneas"
      },
      createButton: "Crear",
      candidateUploadTitle: "Carga de Candidatos",
      multipleFilesTitle: "Archivos Múltiples (PDF/TXT/DOCX)",
      uploadHint: "Soporte para PDF/DOCX/TXT y también CSV en formato: Número de Candidato, Candidato, Número de Orden, Educación, Experiencia, Teléfono, Correo, Ciudad",
      uploadButton: "Subir"
    },
    matchesChat: {
      title: "Coincidencias de Candidatos a Trabajos",
      scoreCalculationTitle: "¿Cómo se calcula la puntuación?",
      showHideButton: "Mostrar/Ocultar",
      searchMatchesTitle: "Buscar Coincidencias por ID",
      searchTypeLabel: "Tipo de Búsqueda",
      candidateRadio: "Candidato",
      jobRadio: "Trabajo",
      idLabel: "ID (24 dígitos hex)",
      exampleId: "Ejemplo: 68af2e02697377761f45e4a6",
      topKLabel: "Top K",
      filterByCityLabel: "Filtrar por Ciudad",
      geographicProximity: "Proximidad Geográfica",
      searchMatchesButton: "Buscar Coincidencias",
      matchingControlsTitle: "Controlar Parámetros de Coincidencia (Actualización en Tiempo Real)",
      proximityFilterLabel: "Filtrar por Proximidad",
      cacheStrategyLabel: "Estrategia de Caché",
      maxAgeLabel: "Edad Máxima (segundos)",
      exampleAge: "Ejemplo: 86400",
      requiredProfessionEscoLabel: "Profesión Requerida ESCO",
      exampleRpEsco: "Ejemplo: 3115",
      fieldOfOccupationEscoLabel: "Campo de Ocupación ESCO",
      exampleFoEsco: "Ejemplo: 7223",
      weightingTitle: "Ponderación de Puntuación y Configuraciones (Servidor)",
      mainWeightsTitle: "Pesos Principales (0-1)",
      weights: {
        skills: "Peso de Habilidades",
        title: "Peso de Título",
        semantic: "Peso Semántico",
        embedding: "Peso de Embedding",
        distance: "Peso de Distancia"
      },
      exampleWeights: "Ejemplo: 0.30",
      categoriesTitle: "Categorías y Límites",
      categoryWeights: {
        required: "Peso de Categoría Requerida",
        nice: "Peso de Categoría Bonita",
        skillsFloor: "Piso de Habilidades (Mínimo)"
      },
      sumHint: "Suma recomendada requerida+bonita = 1",
      exampleCw: "Ejemplo: 0.70",
      buttons: {
        loadFromServer: "Cargar del Servidor",
        normalizeWeights: "Normalizar Pesos",
        applyLocally: "Aplicar Localmente",
        saveToServer: "Guardar en Servidor"
      },
      refreshButton: "Actualizar",
      clearFilterButton: "Limpiar Filtro",
      undoButton: "Deshacer",
      redoButton: "Rehacer",
      tableHeaders: {
        candidate: "Candidato",
        role: "Rol",
        numberOfMatches: "Número de Coincidencias",
        top3: "Top 3"
      },
      chatTitle: "Chat de Coincidencias",
      chatPlaceholder: "Ejemplo: ¿Cuántas coincidencias se encontraron hoy?",
      sendButton: "Enviar",
      chatHint: "El chat usa datos de Mongo y API de OpenAI vía FastAPI"
    },
    copilot: {
      hint: "El asistente está conectado a tu espacio de trabajo. Pregunta cualquier cosa.",
      placeholder: "Mensaje al Copilot...",
      sendButton: "Enviar",
      hint2: "Las respuestas se transmiten en vivo; Copilot puede usar datos y acciones del sistema."
    },
    common: {
      save: "Guardar",
      cancel: "Cancelar",
      loading: "Cargando...",
      error: "Error",
      success: "Éxito",
      notConnected: "No conectado"
    },
    tabs: {
      dashboard: "Tablero",
      jobs: "Trabajos",
      candidates: "Candidatos",
      matches: "Coincidencias+Chat",
      copilot: "Copilot",
      upload: "Subir",
      imports: "Importar CSV",
      logout: "Cerrar sesión"
    }
  }
};

// Current language, default to English
let currentLang = localStorage.getItem('selectedLanguage') || 'en';

// Function to get translated text
function t(key) {
  const keys = key.split('.');
  let value = translations[currentLang];
  for (const k of keys) {
    value = value && value[k];
  }
  return value || key; // Fallback to key if not found
}

// Function to apply translations to elements with data-i18n
function applyTranslations() {
  document.querySelectorAll('[data-i18n]').forEach(el => {
    const key = el.getAttribute('data-i18n');
    el.textContent = t(key);
  });
  document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
    const key = el.getAttribute('data-i18n-placeholder');
    el.placeholder = t(key);
  });
  // Handle titles
  document.querySelectorAll('[data-i18n-title]').forEach(el => {
    const key = el.getAttribute('data-i18n-title');
    el.title = t(key);
  });
}

// Function to set language
function setLanguage(lang) {
  if (translations[lang]) {
    currentLang = lang;
    localStorage.setItem('selectedLanguage', lang);
    applyTranslations();
  }
}

// Export for use in other scripts
window.i18n = { t, applyTranslations, setLanguage, currentLang };
