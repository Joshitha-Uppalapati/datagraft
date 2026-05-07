function ExportPage({ fileId, validationSummary, onRestart }) {
  const downloadUrl = `http://localhost:8000/api/export/${fileId}`;

  return (
    <div className="mt-10 flex justify-center">
      <div className="w-full max-w-md rounded-xl border bg-white p-8 text-center shadow-sm">
        <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-full bg-green-100 text-2xl text-green-600">
          ✓
        </div>

        <h2 className="mb-2 text-xl font-semibold">
          Export Ready
        </h2>

        <p className="mb-4 text-gray-600">
          Your data has been cleaned and is ready for download.
        </p>

        {validationSummary && (
          <p className="mb-6 text-sm text-gray-700">
            Exported {validationSummary.clean_rows} clean rows. Removed{" "}
            {validationSummary.error_rows} error rows.
          </p>
        )}

        <div className="flex flex-col gap-3">
          <a
            href={downloadUrl}
            className="rounded bg-slate-900 px-4 py-2 text-white"
          >
            Download Clean CSV
          </a>

          <a
            href="http://localhost:8000/api/history"
            target="_blank"
            rel="noreferrer"
            className="rounded border px-4 py-2 text-gray-700"
          >
            View History
          </a>

          <button
            onClick={onRestart}
            className="rounded border px-4 py-2 text-gray-700"
          >
            Start New Import
          </button>
        </div>
      </div>
    </div>
  );
}

export default ExportPage;