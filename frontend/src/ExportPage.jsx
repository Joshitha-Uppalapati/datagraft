
function ExportPage({ fileId, onRestart }) {
  const downloadUrl = `http://localhost:8000/api/export/${fileId}`;

  return (
    <div className="mt-10 flex justify-center">
      <div className="w-full max-w-md rounded-xl border bg-white p-8 text-center shadow-sm">
        
        <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-full bg-green-100 text-green-600 text-2xl">
          ✓
        </div>

        <h2 className="text-xl font-semibold mb-2">
          Export Ready
        </h2>

        <p className="text-gray-600 mb-6">
          Your data has been cleaned and is ready for download.
        </p>

        <div className="flex flex-col gap-3">
          <a
            href={downloadUrl}
            className="rounded bg-slate-900 px-4 py-2 text-white"
          >
            Download Clean CSV
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