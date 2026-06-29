import { useState } from "react";
import UploadPage from "./UploadPage";
import MappingPage from "./MappingPage";
import ValidationPage from "./ValidationPage";
import ExportPage from "./ExportPage";
import HistoryPage from "./HistoryPage";

const PAGES = {
  UPLOAD: "upload",
  MAPPING: "mapping",
  VALIDATION: "validation",
  EXPORT: "export",
  HISTORY: "history",
};

const steps = [
  { key: PAGES.UPLOAD, label: "Upload" },
  { key: PAGES.MAPPING, label: "Mapping" },
  { key: PAGES.VALIDATION, label: "Validation" },
  { key: PAGES.EXPORT, label: "Export" },
];

function App() {
  const [page, setPage] = useState(PAGES.UPLOAD);
  const [fileId, setFileId] = useState(null);
  const [detectedColumns, setDetectedColumns] = useState([]);
  const [validationSummary, setValidationSummary] = useState(null);

  const handleUploadSuccess = (data) => {
    setFileId(data.file_id);
    setDetectedColumns(data.detected_columns || data.columns || []);
    setValidationSummary(null);
    setPage(PAGES.MAPPING);
  };

  const handleRestart = () => {
    setPage(PAGES.UPLOAD);
    setFileId(null);
    setDetectedColumns([]);
    setValidationSummary(null);
  };

  const isWizardPage = page !== PAGES.HISTORY;

  return (
    <main className="min-h-screen bg-slate-50 px-6 py-8 text-slate-900">
      <div className="mx-auto max-w-5xl">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h1 className="text-3xl font-semibold">DataGraft</h1>
            <p className="mt-2 text-sm text-slate-500">
              Upload messy files, map columns, validate rows, and export clean data.
            </p>
          </div>

          <button
            onClick={() => setPage(PAGES.HISTORY)}
            className="w-fit rounded border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100"
          >
            View import history
          </button>
        </div>

        {isWizardPage && (
          <div className="mt-6 flex flex-wrap gap-3">
            {steps.map((step, index) => (
              <div
                key={step.key}
                className={`rounded-full px-4 py-2 text-sm ${
                  page === step.key
                    ? "bg-slate-900 text-white"
                    : "border border-slate-200 bg-white text-slate-600"
                }`}
              >
                {index + 1}. {step.label}
              </div>
            ))}
          </div>
        )}

        <section className="mt-8 rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
          {page === PAGES.UPLOAD && (
            <UploadPage onSuccess={handleUploadSuccess} />
          )}

          {page === PAGES.MAPPING && fileId && (
            <MappingPage
              fileId={fileId}
              detectedColumns={detectedColumns}
              onComplete={() => setPage(PAGES.VALIDATION)}
            />
          )}

          {page === PAGES.VALIDATION && fileId && (
            <ValidationPage
              fileId={fileId}
              onProceedToExport={(summary) => {
                setValidationSummary(summary);
                setPage(PAGES.EXPORT);
              }}
            />
          )}

          {page === PAGES.EXPORT && fileId && (
            <ExportPage
              fileId={fileId}
              validationSummary={validationSummary}
              onRestart={handleRestart}
              onViewHistory={() => setPage(PAGES.HISTORY)}
            />
          )}

          {page === PAGES.HISTORY && (
            <HistoryPage onBack={handleRestart} />
          )}
        </section>
      </div>
    </main>
  );
}

export default App;