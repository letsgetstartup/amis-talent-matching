import React from 'react';

interface State { hasError: boolean; error?: any }

export class ErrorBoundary extends React.Component<React.PropsWithChildren, State> {
  constructor(props:any){ super(props); this.state={hasError:false}; }
  static getDerivedStateFromError(error:any){ return { hasError:true, error }; }
  componentDidCatch(err:any, info:any){ console.error('UI Error', err, info); }
  render(){
    if(this.state.hasError){
      return <div className="alert alert-danger" dir="rtl">שגיאה כללית בממשק. רענן את הדף. {String(this.state.error)}</div>;
    }
    return this.props.children;
  }
}

export default ErrorBoundary;
