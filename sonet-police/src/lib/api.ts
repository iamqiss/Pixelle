export type TokenProvider = () => string | undefined

function baseUrl() {
  const url = process.env.NEXT_PUBLIC_MOD_API || ''
  return url.replace(/\/$/, '')
}

export function createApi(token: TokenProvider) {
  async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...(init.headers as any),
    }
    const t = token()
    if (t) headers['Authorization'] = `Bearer ${t}`
    const res = await fetch(`${baseUrl()}${path}`, { ...init, headers, credentials: 'include' })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return (await res.json()) as T
  }

  return {
    streamReports(token?: string) {
      const qs = new URLSearchParams()
      if (token) qs.set('token', token)
      const es = new EventSource(`${baseUrl()}/api/v1/stream/reports?${qs.toString()}`)
      return es
    },
    streamSignals(token?: string) {
      const qs = new URLSearchParams()
      if (token) qs.set('token', token)
      const es = new EventSource(`${baseUrl()}/api/v1/stream/signals?${qs.toString()}`)
      return es
    },
    getReports(params: URLSearchParams) {
      return request<{ success: boolean; data: any[] }>(`/api/v1/reports?${params.toString()}`)
    },
    getAudit(params: URLSearchParams) {
      return request<{ success: boolean; data: any[] }>(`/api/v1/audit?${params.toString()}`)
    },
    getReport(id: string) {
      return request<{ success: boolean; data: any }>(`/api/v1/reports/${id}`)
    },
    updateReportStatus(id: string, status: string) {
      return request<{ success: boolean }>(`/api/v1/reports/${id}/status`, {
        method: 'PUT',
        body: JSON.stringify({ status }),
      })
    },
  }
}

