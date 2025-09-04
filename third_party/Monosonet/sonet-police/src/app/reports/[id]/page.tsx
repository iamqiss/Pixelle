"use client"
import React from 'react'
import { useParams, useRouter } from 'next/navigation'
import { useAuth } from '@/lib/auth'
import { createApi } from '@/lib/api'

export default function ReportDetail() {
  const params = useParams<{ id: string }>()
  const router = useRouter()
  const { state } = useAuth()
  const api = React.useMemo(() => createApi(() => state.token), [state.token])
  const [report, setReport] = React.useState<any>()
  const [loading, setLoading] = React.useState(true)
  const id = params?.id

  const load = React.useCallback(async () => {
    if (!id) return
    setLoading(true)
    const json = await api.getReport(id as string)
    setReport(json.data)
    setLoading(false)
  }, [id, api])

  React.useEffect(() => { load() }, [load])

  const updateStatus = async (status: string) => {
    await api.updateReportStatus(id as string, status)
    await load()
  }

  if (loading) return <main className="p-6">Loading…</main>
  if (!report) return <main className="p-6">Not found</main>

  return (
    <main className="p-6">
      <div className="max-w-4xl mx-auto flex flex-col gap-4">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-semibold">Report {report.id}</h1>
          <div className="flex gap-2">
            <button className="px-3 py-1 rounded bg-amber-600 text-white" onClick={() => updateStatus('under_investigation')}>Start Investigation</button>
            <button className="px-3 py-1 rounded bg-green-600 text-white" onClick={() => updateStatus('resolved')}>Resolve</button>
            <button className="px-3 py-1 rounded bg-gray-700 text-white" onClick={() => updateStatus('dismissed')}>Dismiss</button>
          </div>
        </div>
        <div className="border rounded bg-white p-4 text-sm">
          <div><span className="text-gray-500">Type:</span> {report.report_type}</div>
          <div><span className="text-gray-500">Priority:</span> {report.priority}</div>
          <div><span className="text-gray-500">Status:</span> {report.status}</div>
          <div><span className="text-gray-500">Reason:</span> {report.reason}</div>
          <div><span className="text-gray-500">Reporter:</span> {report.reporter_id}</div>
          <div><span className="text-gray-500">Target:</span> {report.target_id}</div>
          <div><span className="text-gray-500">Created:</span> {new Date(report.created_at).toLocaleString()}</div>
        </div>
        <button className="px-3 py-1 rounded bg-black text-white w-max" onClick={() => router.back()}>Back</button>
      </div>
    </main>
  )
}

