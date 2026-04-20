import { useState } from "react"

// Page flow: upload → mapping → validation → export
// Using simple string state instead of a router to keep 
// dependencies minimal — this is a single-flow wizard, not a SPA.
const PAGES = {
  UPLOAD: "upload",
  MAPPING: "mapping",
  VALIDATION: "validation",
  EXPORT: "export",
}

export default function App() {
  const [page, setPage] = useState(PAGES.UPLOAD)
  const [sessionData, setSessionData] = useState(null)

  return (
    <div className="min-h-screen bg-gray-50">
      {page === PAGES.UPLOAD && (
        <div>Upload page coming soon</div>
      )}
      {page === PAGES.MAPPING && (
        <div>Mapping page coming soon</div>
      )}
      {page === PAGES.VALIDATION && (
        <div>Validation page coming soon</div>
      )}
    </div>
  )
}